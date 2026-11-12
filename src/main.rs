/*----------------------
 | main.rs
 | Description: Command-line entry point: provision the pipeline, submit an order, or run the worker against AWS or a server-mode emulator.
 | Author: seanie.gleason
 | Dependencies: order_pipeline (src/pipeline), aws-config, tokio
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/

use std::env;

use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use order_pipeline::PipelineResult;
use order_pipeline::clients::Clients;
use order_pipeline::names::{INCOMING_PREFIX, Names};
use order_pipeline::provision::provision;
use order_pipeline::worker::process_batch;

/*----------------------
 | USAGE
 | Description: Help text printed for an unknown or missing command.
 | Author: seanie.gleason
 | Dependencies: N/A
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/
const USAGE: &str = "usage: order-pipeline <provision | submit <order.json> | work>
  PIPELINE_PREFIX  resource name prefix (default: demo)
  AWS_ENDPOINT_URL set to https://localhost:9297 to use a server-mode emulator";

/*----------------------
 | main
 | Description: Loads the default AWS config from the environment and runs the requested command.
 | Author: seanie.gleason
 | Dependencies: aws-config
 | Globals: PIPELINE_PREFIX environment variable
 | Params: N/A
 | Returns: unit, or the first error
 ----------------------*/
#[tokio::main]
async fn main() -> PipelineResult<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let prefix = env::var("PIPELINE_PREFIX").unwrap_or_else(|_| "demo".into());
    let names = Names::with_prefix(&prefix);
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let clients = Clients::from_config(&config);

    match args.first().map(String::as_str) {
        Some("provision") => {
            let resources = provision(&clients, &names).await?;
            println!("{resources:#?}");
        }
        Some("submit") => {
            let path = args.get(1).ok_or(USAGE)?;
            submit(&clients, &names, path).await?;
        }
        Some("work") => {
            let resources = names.resolve(&clients).await?;
            loop {
                for outcome in process_batch(&clients, &resources, 20).await? {
                    println!("{outcome:?}");
                }
            }
        }
        _ => eprintln!("{USAGE}"),
    }
    Ok(())
}

/*----------------------
 | submit
 | Description: Uploads an order file under the incoming prefix, which triggers the pipeline.
 | Author: seanie.gleason
 | Dependencies: aws-sdk-s3
 | Globals: N/A
 | Params: clients -- the SDK clients; names -- the resource names; path -- the local order JSON file
 | Returns: unit
 ----------------------*/
async fn submit(clients: &Clients, names: &Names, path: &str) -> PipelineResult<()> {
    let file_name = std::path::Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("order path has no file name")?;
    let key = format!("{INCOMING_PREFIX}{file_name}");
    clients
        .s3
        .put_object()
        .bucket(&names.bucket)
        .key(&key)
        .content_type("application/json")
        .body(ByteStream::from_path(path).await?)
        .send()
        .await?;
    println!("uploaded s3://{}/{key}", names.bucket);
    Ok(())
}
