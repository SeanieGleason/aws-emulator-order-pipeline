/*----------------------
 | worker.rs
 | Description: Drains the orders queue: reads each uploaded order, stores it in DynamoDB, writes a receipt, and announces it on SNS.
 | Author: seanie.gleason
 | Dependencies: clients.rs, names.rs, order.rs, lib.rs, aws-sdk-*
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_sqs::types::Message;
use serde::Deserialize;
use serde_json::json;

use crate::PipelineResult;
use crate::clients::Clients;
use crate::names::{RECEIPT_PREFIX, Resources};
use crate::order::Order;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Processed { order_id: String },
    Duplicate { order_id: String },
    Rejected { key: String, reason: String },
    Ignored,
}

#[derive(Debug, Deserialize)]
struct S3Event {
    #[serde(rename = "Records", default)]
    records: Vec<S3EventRecord>,
}

#[derive(Debug, Deserialize)]
struct S3EventRecord {
    s3: S3Entity,
}

#[derive(Debug, Deserialize)]
struct S3Entity {
    bucket: S3Bucket,
    object: S3Object,
}

#[derive(Debug, Deserialize)]
struct S3Bucket {
    name: String,
}

#[derive(Debug, Deserialize)]
struct S3Object {
    key: String,
}

/*----------------------
 | process_batch
 | Description: Receives up to ten messages and handles each; rejected ones are made visible again so SQS can dead-letter them.
 | Author: seanie.gleason
 | Dependencies: clients.rs, names.rs
 | Globals: N/A
 | Params: clients -- the SDK clients; resources -- the pipeline; wait_seconds -- long-poll time, 0 to 20
 | Returns: one outcome per S3 record seen
 ----------------------*/
pub async fn process_batch(
    clients: &Clients,
    resources: &Resources,
    wait_seconds: i32,
) -> PipelineResult<Vec<Outcome>> {
    let received = clients
        .sqs
        .receive_message()
        .queue_url(&resources.orders_queue_url)
        .max_number_of_messages(10)
        .wait_time_seconds(wait_seconds)
        .send()
        .await?;

    let mut outcomes = Vec::new();
    for message in received.messages() {
        let message_outcomes = handle_message(clients, resources, message).await?;
        let receipt_handle = message.receipt_handle().ok_or("message has no receipt handle")?;
        if message_outcomes.iter().any(|o| matches!(o, Outcome::Rejected { .. })) {
            clients
                .sqs
                .change_message_visibility()
                .queue_url(&resources.orders_queue_url)
                .receipt_handle(receipt_handle)
                .visibility_timeout(0)
                .send()
                .await?;
        } else {
            clients
                .sqs
                .delete_message()
                .queue_url(&resources.orders_queue_url)
                .receipt_handle(receipt_handle)
                .send()
                .await?;
        }
        outcomes.extend(message_outcomes);
    }
    Ok(outcomes)
}

/*----------------------
 | handle_message
 | Description: Handles every S3 record in one SQS message; an S3 test event or other non-record body is ignored.
 | Author: seanie.gleason
 | Dependencies: serde_json
 | Globals: N/A
 | Params: clients -- the SDK clients; resources -- the pipeline; message -- the SQS message
 | Returns: one outcome per record, or a single Ignored
 ----------------------*/
async fn handle_message(
    clients: &Clients,
    resources: &Resources,
    message: &Message,
) -> PipelineResult<Vec<Outcome>> {
    let event: S3Event = match serde_json::from_str(message.body().unwrap_or_default()) {
        Ok(event) => event,
        Err(_) => return Ok(vec![Outcome::Ignored]),
    };
    if event.records.is_empty() {
        return Ok(vec![Outcome::Ignored]);
    }
    let mut outcomes = Vec::with_capacity(event.records.len());
    for record in &event.records {
        let key = decode_key(&record.s3.object.key);
        outcomes.push(process_order(clients, resources, &record.s3.bucket.name, &key).await?);
    }
    Ok(outcomes)
}

/*----------------------
 | process_order
 | Description: Reads one uploaded order and, if valid and new, stores it, writes its receipt and announces it.
 | Author: seanie.gleason
 | Dependencies: order.rs
 | Globals: N/A
 | Params: clients -- the SDK clients; resources -- the pipeline; bucket, key -- the uploaded object
 | Returns: the outcome for this order
 ----------------------*/
async fn process_order(
    clients: &Clients,
    resources: &Resources,
    bucket: &str,
    key: &str,
) -> PipelineResult<Outcome> {
    let bytes = clients
        .s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    let order = match Order::parse(&bytes) {
        Ok(order) => order,
        Err(reason) => {
            return Ok(Outcome::Rejected {
                key: key.to_string(),
                reason,
            });
        }
    };
    if !record_order(clients, resources, &order).await? {
        return Ok(Outcome::Duplicate {
            order_id: order.order_id,
        });
    }
    write_receipt(clients, resources, &order).await?;
    announce(clients, resources, &order).await?;
    Ok(Outcome::Processed {
        order_id: order.order_id,
    })
}

/*----------------------
 | record_order
 | Description: Writes the order once; a condition on order_id makes SQS redelivery harmless.
 | Author: seanie.gleason
 | Dependencies: aws-sdk-dynamodb
 | Globals: N/A
 | Params: clients -- the SDK clients; resources -- the pipeline; order -- the valid order
 | Returns: true if written, false if the order was already stored
 ----------------------*/
async fn record_order(
    clients: &Clients,
    resources: &Resources,
    order: &Order,
) -> PipelineResult<bool> {
    let result = clients
        .dynamodb
        .put_item()
        .table_name(&resources.table)
        .item("order_id", AttributeValue::S(order.order_id.clone()))
        .item("customer", AttributeValue::S(order.customer.clone()))
        .item("total_cents", AttributeValue::N(order.total_cents().to_string()))
        .item("line_items", AttributeValue::N(order.items.len().to_string()))
        .item("status", AttributeValue::S("PROCESSED".into()))
        .condition_expression("attribute_not_exists(order_id)")
        .send()
        .await;
    match result {
        Ok(_) => Ok(true),
        Err(e)
            if e.as_service_error()
                .is_some_and(|s| s.is_conditional_check_failed_exception()) =>
        {
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

/*----------------------
 | write_receipt
 | Description: Stores a JSON receipt for the order under the receipts prefix, outside the trigger prefix.
 | Author: seanie.gleason
 | Dependencies: aws-sdk-s3, names.rs
 | Globals: N/A
 | Params: clients -- the SDK clients; resources -- the pipeline; order -- the stored order
 | Returns: unit
 ----------------------*/
async fn write_receipt(
    clients: &Clients,
    resources: &Resources,
    order: &Order,
) -> PipelineResult<()> {
    let receipt = json!({
        "order_id": order.order_id,
        "customer": order.customer,
        "total_cents": order.total_cents(),
    });
    clients
        .s3
        .put_object()
        .bucket(&resources.bucket)
        .key(receipt_key(&order.order_id))
        .content_type("application/json")
        .body(ByteStream::from(receipt.to_string().into_bytes()))
        .send()
        .await?;
    Ok(())
}

/*----------------------
 | announce
 | Description: Publishes an order-processed message to the SNS topic.
 | Author: seanie.gleason
 | Dependencies: aws-sdk-sns
 | Globals: N/A
 | Params: clients -- the SDK clients; resources -- the pipeline; order -- the stored order
 | Returns: unit
 ----------------------*/
async fn announce(clients: &Clients, resources: &Resources, order: &Order) -> PipelineResult<()> {
    let message = json!({
        "order_id": order.order_id,
        "total_cents": order.total_cents(),
    });
    clients
        .sns
        .publish()
        .topic_arn(&resources.topic_arn)
        .message(message.to_string())
        .send()
        .await?;
    Ok(())
}

/*----------------------
 | receipt_key
 | Description: The S3 key a receipt is written to for one order.
 | Author: seanie.gleason
 | Dependencies: names.rs
 | Globals: N/A
 | Params: order_id -- the order id
 | Returns: the receipt key
 ----------------------*/
pub fn receipt_key(order_id: &str) -> String {
    format!("{RECEIPT_PREFIX}{order_id}.json")
}

/*----------------------
 | decode_key
 | Description: Undoes the URL encoding S3 applies to object keys in event notifications, where a space arrives as '+'.
 | Author: seanie.gleason
 | Dependencies: N/A
 | Globals: N/A
 | Params: raw -- the key as it appears in the event
 | Returns: the object key
 ----------------------*/
pub fn decode_key(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => decoded.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        decoded.push(byte);
                        i += 2;
                    }
                    Err(_) => decoded.push(b'%'),
                }
            }
            other => decoded.push(other),
        }
        i += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}
