/*----------------------
 | provision.rs
 | Description: Creates the bucket, table, topic and queues, and wires S3 to SQS, SNS to SQS, and SQS to its dead-letter queue.
 | Author: seanie.gleason
 | Dependencies: clients.rs, names.rs, lib.rs, aws-sdk-*
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/

use std::time::Duration;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType, TableStatus,
};
use aws_sdk_s3::types::{
    Event, FilterRule, FilterRuleName, NotificationConfiguration, NotificationConfigurationFilter,
    QueueConfiguration, S3KeyFilter,
};
use aws_sdk_sqs::types::QueueAttributeName;
use serde_json::json;

use crate::PipelineResult;
use crate::clients::Clients;
use crate::names::{INCOMING_PREFIX, Names, Resources};

/*----------------------
 | MAX_RECEIVE_COUNT
 | Description: Deliveries an order gets before SQS moves it to the dead-letter queue.
 | Author: seanie.gleason
 | Dependencies: N/A
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/
pub const MAX_RECEIVE_COUNT: u32 = 2;

/*----------------------
 | provision
 | Description: Creates every pipeline resource and returns the handles the worker needs.
 | Author: seanie.gleason
 | Dependencies: clients.rs, names.rs
 | Globals: N/A
 | Params: clients -- the SDK clients; names -- the resource names
 | Returns: the provisioned resources
 ----------------------*/
pub async fn provision(clients: &Clients, names: &Names) -> PipelineResult<Resources> {
    clients.s3.create_bucket().bucket(&names.bucket).send().await?;
    create_table(clients, &names.table).await?;

    let topic_arn = clients
        .sns
        .create_topic()
        .name(&names.topic)
        .send()
        .await?
        .topic_arn()
        .ok_or("CreateTopic returned no TopicArn")?
        .to_string();

    let dead_letter_queue_url = create_queue(clients, &names.dead_letter_queue, &[]).await?;
    let dead_letter_queue_arn = queue_arn(clients, &dead_letter_queue_url).await?;

    let orders_queue_url = create_queue(
        clients,
        &names.orders_queue,
        &[
            (QueueAttributeName::VisibilityTimeout, "30".to_string()),
            (
                QueueAttributeName::RedrivePolicy,
                json!({
                    "deadLetterTargetArn": dead_letter_queue_arn,
                    "maxReceiveCount": MAX_RECEIVE_COUNT.to_string(),
                })
                .to_string(),
            ),
        ],
    )
    .await?;
    let orders_queue_arn = queue_arn(clients, &orders_queue_url).await?;
    allow_sender(
        clients,
        &orders_queue_url,
        &orders_queue_arn,
        "s3.amazonaws.com",
        &format!("arn:aws:s3:::{}", names.bucket),
    )
    .await?;

    let notifications_queue_url = create_queue(clients, &names.notifications_queue, &[]).await?;
    let notifications_queue_arn = queue_arn(clients, &notifications_queue_url).await?;
    allow_sender(
        clients,
        &notifications_queue_url,
        &notifications_queue_arn,
        "sns.amazonaws.com",
        &topic_arn,
    )
    .await?;
    clients
        .sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&notifications_queue_arn)
        .attributes("RawMessageDelivery", "true")
        .send()
        .await?;

    notify_queue_on_upload(clients, &names.bucket, &orders_queue_arn).await?;

    Ok(Resources {
        bucket: names.bucket.clone(),
        table: names.table.clone(),
        topic_arn,
        orders_queue_url,
        dead_letter_queue_url,
        notifications_queue_url,
    })
}

/*----------------------
 | create_table
 | Description: Creates the on-demand orders table keyed by order_id and waits until it is ACTIVE.
 | Author: seanie.gleason
 | Dependencies: aws-sdk-dynamodb
 | Globals: N/A
 | Params: clients -- the SDK clients; table -- the table name
 | Returns: unit
 ----------------------*/
async fn create_table(clients: &Clients, table: &str) -> PipelineResult<()> {
    clients
        .dynamodb
        .create_table()
        .table_name(table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("order_id")
                .attribute_type(ScalarAttributeType::S)
                .build()?,
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("order_id")
                .key_type(KeyType::Hash)
                .build()?,
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await?;

    for _ in 0..60 {
        let described = clients.dynamodb.describe_table().table_name(table).send().await?;
        if described.table().and_then(|t| t.table_status()) == Some(&TableStatus::Active) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(format!("table {table} never became ACTIVE").into())
}

/*----------------------
 | create_queue
 | Description: Creates a standard queue with the given attributes.
 | Author: seanie.gleason
 | Dependencies: aws-sdk-sqs
 | Globals: N/A
 | Params: clients -- the SDK clients; name -- the queue name; attributes -- queue attributes to set
 | Returns: the queue URL
 ----------------------*/
async fn create_queue(
    clients: &Clients,
    name: &str,
    attributes: &[(QueueAttributeName, String)],
) -> PipelineResult<String> {
    let mut request = clients.sqs.create_queue().queue_name(name);
    for (key, value) in attributes {
        request = request.attributes(key.clone(), value);
    }
    Ok(request
        .send()
        .await?
        .queue_url()
        .ok_or("CreateQueue returned no QueueUrl")?
        .to_string())
}

/*----------------------
 | queue_arn
 | Description: Reads a queue's ARN, which S3, SNS and the redrive policy address it by.
 | Author: seanie.gleason
 | Dependencies: aws-sdk-sqs
 | Globals: N/A
 | Params: clients -- the SDK clients; queue_url -- the queue URL
 | Returns: the queue ARN
 ----------------------*/
async fn queue_arn(clients: &Clients, queue_url: &str) -> PipelineResult<String> {
    let response = clients
        .sqs
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await?;
    Ok(response
        .attributes()
        .and_then(|a| a.get(&QueueAttributeName::QueueArn))
        .ok_or("GetQueueAttributes returned no QueueArn")?
        .clone())
}

/*----------------------
 | allow_sender
 | Description: Sets a queue policy letting one AWS service send to the queue, but only on behalf of one source ARN.
 | Author: seanie.gleason
 | Dependencies: aws-sdk-sqs, serde_json
 | Globals: N/A
 | Params: clients -- the SDK clients; queue_url, queue_arn -- the queue; service -- the sending service principal; source_arn -- the resource allowed to send
 | Returns: unit
 ----------------------*/
async fn allow_sender(
    clients: &Clients,
    queue_url: &str,
    queue_arn: &str,
    service: &str,
    source_arn: &str,
) -> PipelineResult<()> {
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "Service": service },
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": { "ArnEquals": { "aws:SourceArn": source_arn } }
        }]
    });
    clients
        .sqs
        .set_queue_attributes()
        .queue_url(queue_url)
        .attributes(QueueAttributeName::Policy, policy.to_string())
        .send()
        .await?;
    Ok(())
}

/*----------------------
 | notify_queue_on_upload
 | Description: Sends an S3 ObjectCreated event to the orders queue for every key under the incoming prefix.
 | Author: seanie.gleason
 | Dependencies: aws-sdk-s3, names.rs
 | Globals: N/A
 | Params: clients -- the SDK clients; bucket -- the bucket; orders_queue_arn -- the destination queue
 | Returns: unit
 ----------------------*/
async fn notify_queue_on_upload(
    clients: &Clients,
    bucket: &str,
    orders_queue_arn: &str,
) -> PipelineResult<()> {
    let only_incoming = NotificationConfigurationFilter::builder()
        .key(
            S3KeyFilter::builder()
                .filter_rules(
                    FilterRule::builder()
                        .name(FilterRuleName::Prefix)
                        .value(INCOMING_PREFIX)
                        .build(),
                )
                .build(),
        )
        .build();
    let configuration = NotificationConfiguration::builder()
        .queue_configurations(
            QueueConfiguration::builder()
                .queue_arn(orders_queue_arn)
                .events(Event::from("s3:ObjectCreated:*"))
                .filter(only_incoming)
                .build()?,
        )
        .build();
    clients
        .s3
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(configuration)
        .send()
        .await?;
    Ok(())
}
