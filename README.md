# order-pipeline

A small event-driven AWS app, tested end to end against [AWS Emulator](https://github.com/aws/aws-emulator) with no AWS account.

```
PUT incoming/order.json ─► S3 ─(ObjectCreated)─► SQS orders ─► worker ─► DynamoDB orders
                                                   │              ├─► S3 receipts/<id>.json
                                     (2 failures)  ▼              └─► SNS order-processed ─► SQS notifications
                                              SQS orders-dlq
```

## Layout

| Path | What |
|---|---|
| `src/main.rs` | CLI: `provision`, `submit <file>`, `work` |
| `src/pipeline/provision.rs` | Creates and wires every resource |
| `src/pipeline/worker.rs` | Drains the orders queue |
| `src/pipeline/order.rs` | Order document and validation |
| `tests/pipeline.rs` | End-to-end tests, one in-process emulator each |

## Test (embedded mode)

The tests link the emulator into the test binary through `aws-emulator-embedded`, which expects a checkout of `aws-emulator` beside this folder (see `Cargo.toml`).

```bash
cargo test
```

The first build compiles every emulator service crate, so expect a long cold build and a large `target/`.

## Run (server mode)

Start the emulator (Docker on Windows and macOS, the package on Linux), then:

```bash
export AWS_ENDPOINT_URL=https://localhost:9297 AWS_REGION=us-east-1
export AWS_CA_BUNDLE=/path/to/emulator-ca.pem
cargo run -- provision
cargo run -- submit samples/order-1001.json
cargo run -- work
```

Unset `AWS_ENDPOINT_URL` and the same binary runs against real AWS.
