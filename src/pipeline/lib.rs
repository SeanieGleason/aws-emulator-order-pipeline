/*----------------------
 | lib.rs
 | Description: Library root of the order pipeline: S3 upload to SQS, a worker that stores orders in DynamoDB, and an SNS announcement.
 | Author: seanie.gleason
 | Dependencies: clients.rs, names.rs, order.rs, provision.rs, worker.rs
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/

pub mod clients;
pub mod names;
pub mod order;
pub mod provision;
pub mod worker;

/*----------------------
 | PipelineResult
 | Description: Result alias shared by every pipeline call, boxing SDK and parse errors alike.
 | Author: seanie.gleason
 | Dependencies: N/A
 | Globals: N/A
 | Params: T -- the success value
 | Returns: N/A
 ----------------------*/
pub type PipelineResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
