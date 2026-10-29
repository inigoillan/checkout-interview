# Checkout Count

The checkout-count module aggregates view events from the [event-schema](../../checkout-event-schema) module to calculate 1 minute count over the following dimensions: postcode and time down to a 1 minute.

## Technical Requirement

This module uses Flink and Java as the base to perform the aggregations required. The data needs to be processed and stored in S3. Output data will be stored partitioned by day and hour in S3.

### Source code arrangement

Use of SOLID principles is encouraged to make different components decoupled and testable.

### Testing strategy

We require 3 different types of test: unit, integration tests and end to end tests.

This project needs to use JUnit 5 to run all the tests.
For integration and end to end tests, we will use Testcontainers to run localstack to simulate S3 and Apache Kafka images.

Unit test should run without any S3 or Kafka considerations
Integration test should run with either S3 (via localstack) or Kafka but never both in the same test.
End to end tests should run using Kafka, S3 (via localstack)

### How Flink should be setup

Flink needs to be setup using:
- End to end (Kafka -> S3), exactly once semantics so we don't end up with duplicated data
- In order to manage out of order and late message arrival, we will use watermarks. The watermarks should use the view event timestamp and allow events to be processed up to 10 minute after arrival after which we will discard those records and store them in a different folder for inspection.
- Checkpointing is setup with the following features:
  - Unaligned Checkpoints
  - State checkpointing stored using RocksDB
- Given the expected amount of data (100k events per day), we won't be considering any strategies to avoid overload of nodes due to hot partitions