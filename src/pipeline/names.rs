/*----------------------
 | names.rs
 | Description: Resource names derived from one prefix, and the resolved handles (queue URLs, topic ARN) the worker needs.
 | Author: seanie.gleason
 | Dependencies: clients.rs, lib.rs
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/

use crate::PipelineResult;
use crate::clients::Clients;

/*----------------------
 | INCOMING_PREFIX
 | Description: Key prefix whose uploads trigger the pipeline.
 | Author: seanie.gleason
 | Dependencies: N/A
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/
pub const INCOMING_PREFIX: &str = "incoming/";

/*----------------------
 | RECEIPT_PREFIX
 | Description: Key prefix the worker writes receipts under.
 | Author: seanie.gleason
 | Dependencies: N/A
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/
pub const RECEIPT_PREFIX: &str = "receipts/";

#[derive(Debug, Clone)]
pub struct Names {
    pub bucket: String,
    pub table: String,
    pub topic: String,
    pub orders_queue: String,
    pub dead_letter_queue: String,
    pub notifications_queue: String,
}

#[derive(Debug, Clone)]
pub struct Resources {
    pub bucket: String,
    pub table: String,
    pub topic_arn: String,
    pub orders_queue_url: String,
    pub dead_letter_queue_url: String,
    pub notifications_queue_url: String,
}

impl Names {
    /*----------------------
     | with_prefix
     | Description: Derives every resource name from one prefix so several pipelines can share an account.
     | Author: seanie.gleason
     | Dependencies: N/A
     | Globals: N/A
     | Params: prefix -- lowercase letters, digits and hyphens
     | Returns: the names
     ----------------------*/
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            bucket: format!("{prefix}-orders"),
            table: format!("{prefix}-orders"),
            topic: format!("{prefix}-order-processed"),
            orders_queue: format!("{prefix}-orders"),
            dead_letter_queue: format!("{prefix}-orders-dlq"),
            notifications_queue: format!("{prefix}-order-notifications"),
        }
    }

    /*----------------------
     | resolve
     | Description: Looks up the queue URLs and topic ARN of an already provisioned pipeline.
     | Author: seanie.gleason
     | Dependencies: clients.rs
     | Globals: N/A
     | Params: clients -- the SDK clients
     | Returns: the resolved resources
     ----------------------*/
    pub async fn resolve(&self, clients: &Clients) -> PipelineResult<Resources> {
        let topic_arn = clients
            .sns
            .create_topic()
            .name(&self.topic)
            .send()
            .await?
            .topic_arn()
            .ok_or("CreateTopic returned no TopicArn")?
            .to_string();
        Ok(Resources {
            bucket: self.bucket.clone(),
            table: self.table.clone(),
            topic_arn,
            orders_queue_url: queue_url(clients, &self.orders_queue).await?,
            dead_letter_queue_url: queue_url(clients, &self.dead_letter_queue).await?,
            notifications_queue_url: queue_url(clients, &self.notifications_queue).await?,
        })
    }
}

/*----------------------
 | queue_url
 | Description: Returns the URL of an existing queue by name.
 | Author: seanie.gleason
 | Dependencies: clients.rs
 | Globals: N/A
 | Params: clients -- the SDK clients; name -- the queue name
 | Returns: the queue URL
 ----------------------*/
async fn queue_url(clients: &Clients, name: &str) -> PipelineResult<String> {
    Ok(clients
        .sqs
        .get_queue_url()
        .queue_name(name)
        .send()
        .await?
        .queue_url()
        .ok_or("GetQueueUrl returned no QueueUrl")?
        .to_string())
}
