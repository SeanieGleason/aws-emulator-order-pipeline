/*----------------------
 | clients.rs
 | Description: Builds the four AWS SDK clients the pipeline uses from one shared SdkConfig.
 | Author: seanie.gleason
 | Dependencies: aws-config, aws-sdk-s3, aws-sdk-sqs, aws-sdk-sns, aws-sdk-dynamodb
 | Globals: N/A
 | Params: N/A
 | Returns: N/A
 ----------------------*/

use aws_config::SdkConfig;

pub struct Clients {
    pub s3: aws_sdk_s3::Client,
    pub sqs: aws_sdk_sqs::Client,
    pub sns: aws_sdk_sns::Client,
    pub dynamodb: aws_sdk_dynamodb::Client,
}

impl Clients {
    /*----------------------
     | from_config
     | Description: Creates every client from one config; S3 uses path-style addressing so a localhost endpoint works too.
     | Author: seanie.gleason
     | Dependencies: aws-config
     | Globals: N/A
     | Params: config -- the SDK config, pointed at AWS or at the emulator
     | Returns: the four clients
     ----------------------*/
    pub fn from_config(config: &SdkConfig) -> Self {
        let s3_config = aws_sdk_s3::config::Builder::from(config)
            .force_path_style(true)
            .build();
        Self {
            s3: aws_sdk_s3::Client::from_conf(s3_config),
            sqs: aws_sdk_sqs::Client::new(config),
            sns: aws_sdk_sns::Client::new(config),
            dynamodb: aws_sdk_dynamodb::Client::new(config),
        }
    }
}
