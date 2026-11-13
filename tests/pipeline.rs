/*----------------------
 | pipeline.rs
 | Description: End-to-end tests of the order pipeline, each against its own in-process AWS Emulator.
 | Author: seanie.gleason
 | Dependencies: order_pipeline, aws-emulator-embedded, aws-config, tokio
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_emulator_embedded::{AwsEmulatorEmbedded, EmbeddedOptions, Telemetry};
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3::primitives::ByteStream;
use order_pipeline::PipelineResult;
use order_pipeline::clients::Clients;
use order_pipeline::names::{INCOMING_PREFIX, Names, Resources};
use order_pipeline::provision::{MAX_RECEIVE_COUNT, provision};
use order_pipeline::worker::{Outcome, decode_key, process_batch, receipt_key};
use serde_json::{Value, json};

async fn sdk_config(emulator: &AwsEmulatorEmbedded) -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .http_client(emulator.http_client())
        .credentials_provider(emulator.aws_credentials_provider())
        .region(Region::new("us-east-1"))
        .load()
        .await
}

async fn fresh_pipeline(prefix: &str) -> (AwsEmulatorEmbedded, Clients, Resources) {
    let emulator = AwsEmulatorEmbedded::new(EmbeddedOptions::default().telemetry(Telemetry::Off));
    let clients = Clients::from_config(&sdk_config(&emulator).await);
    let resources = provision(&clients, &Names::with_prefix(prefix))
        .await
        .expect("provision the pipeline");
    (emulator, clients, resources)
}

async fn upload(clients: &Clients, resources: &Resources, name: &str, body: Value) {
    clients
        .s3
        .put_object()
        .bucket(&resources.bucket)
        .key(format!("{INCOMING_PREFIX}{name}"))
        .body(ByteStream::from(body.to_string().into_bytes()))
        .send()
        .await
        .expect("upload the order");
}

async fn work_until<F>(clients: &Clients, resources: &Resources, done: F) -> Vec<Outcome>
where
    F: Fn(&[Outcome]) -> bool,
{
    let mut seen = Vec::new();
    for _ in 0..20 {
        seen.extend(process_batch(clients, resources, 1).await.expect("process a batch"));
        if done(&seen) {
            return seen;
        }
    }
    panic!("worker never reached the expected state; saw {seen:?}");
}

async fn receive_one(clients: &Clients, queue_url: &str) -> PipelineResult<Option<String>> {
    for _ in 0..10 {
        let received = clients
            .sqs
            .receive_message()
            .queue_url(queue_url)
            .wait_time_seconds(1)
            .send()
            .await?;
        if let Some(body) = received.messages().first().and_then(|m| m.body()) {
            return Ok(Some(body.to_string()));
        }
    }
    Ok(None)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_valid_order_is_stored_receipted_and_announced() {
    let (_emulator, clients, resources) = fresh_pipeline("valid").await;

    upload(
        &clients,
        &resources,
        "order-1001.json",
        json!({
            "order_id": "1001",
            "customer": "ada@example.com",
            "items": [
                { "sku": "KB-01", "quantity": 2, "unit_price_cents": 4999 },
                { "sku": "MS-07", "quantity": 1, "unit_price_cents": 2500 }
            ]
        }),
    )
    .await;

    work_until(&clients, &resources, |seen| {
        seen.contains(&Outcome::Processed { order_id: "1001".into() })
    })
    .await;

    let item = clients
        .dynamodb
        .get_item()
        .table_name(&resources.table)
        .key("order_id", AttributeValue::S("1001".into()))
        .send()
        .await
        .expect("get the order")
        .item
        .expect("order is stored");
    assert_eq!(item["status"], AttributeValue::S("PROCESSED".into()));
    assert_eq!(item["total_cents"], AttributeValue::N("12498".into()));

    clients
        .s3
        .head_object()
        .bucket(&resources.bucket)
        .key(receipt_key("1001"))
        .send()
        .await
        .expect("receipt exists");

    let announced = receive_one(&clients, &resources.notifications_queue_url)
        .await
        .expect("receive the notification")
        .expect("SNS delivered to the subscribed queue");
    let announced: Value = serde_json::from_str(&announced).expect("raw JSON message");
    assert_eq!(announced, json!({ "order_id": "1001", "total_cents": 12498 }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_redelivered_order_is_stored_once() {
    let (_emulator, clients, resources) = fresh_pipeline("dupe").await;
    let order = json!({
        "order_id": "2002",
        "customer": "grace@example.com",
        "items": [{ "sku": "CB-11", "quantity": 1, "unit_price_cents": 899 }]
    });

    upload(&clients, &resources, "order-2002.json", order.clone()).await;
    upload(&clients, &resources, "order-2002-retry.json", order).await;

    let processed = Outcome::Processed { order_id: "2002".into() };
    let duplicate = Outcome::Duplicate { order_id: "2002".into() };
    let seen = work_until(&clients, &resources, |seen| {
        seen.contains(&processed) && seen.contains(&duplicate)
    })
    .await;
    assert_eq!(seen.iter().filter(|o| **o == processed).count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_order_lands_in_the_dead_letter_queue() {
    let (_emulator, clients, resources) = fresh_pipeline("poison").await;

    upload(
        &clients,
        &resources,
        "order-3003.json",
        json!({ "order_id": "3003", "customer": "linus@example.com", "items": [] }),
    )
    .await;

    let seen = work_until(&clients, &resources, |seen| {
        seen.iter()
            .filter(|o| matches!(o, Outcome::Rejected { .. }))
            .count()
            >= MAX_RECEIVE_COUNT as usize
    })
    .await;
    assert!(seen.iter().all(|o| !matches!(o, Outcome::Processed { .. })));

    let after_redrive = process_batch(&clients, &resources, 1)
        .await
        .expect("receive past the redrive limit");
    assert!(after_redrive.is_empty());

    let dead_letter = receive_one(&clients, &resources.dead_letter_queue_url)
        .await
        .expect("receive from the dead-letter queue")
        .expect("SQS redrove the poison message");
    assert!(dead_letter.contains("order-3003.json"));

    let stored = clients
        .dynamodb
        .get_item()
        .table_name(&resources.table)
        .key("order_id", AttributeValue::S("3003".into()))
        .send()
        .await
        .expect("get the order");
    assert!(stored.item.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn two_emulators_never_share_state() {
    let (_emulator, _clients, _resources) = fresh_pipeline("isolated").await;

    let other = AwsEmulatorEmbedded::new(EmbeddedOptions::default().telemetry(Telemetry::Off));
    let other_clients = Clients::from_config(&sdk_config(&other).await);
    let buckets = other_clients
        .s3
        .list_buckets()
        .send()
        .await
        .expect("list buckets");
    assert!(buckets.buckets().is_empty());
}

#[test]
fn notification_keys_are_url_decoded() {
    assert_eq!(decode_key("incoming/order+1%2B2.json"), "incoming/order 1+2.json");
    assert_eq!(decode_key("incoming/100%"), "incoming/100%");
}
