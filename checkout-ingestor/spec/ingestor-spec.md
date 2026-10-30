# Checkout Ingestor

## Functional requirements

The checkout-ingestor module loads view events from the [event-schema](../../checkout-event-schema) module into the datalake to be consumed by batch processes.


## Technical Requirement

This module uses Flink and Java as the base to perform the aggregations required.


### Input data

Data will be read from the `checkout.events.raw` topic following the view event schema as defined on the [event-schema](../../checkout-event-schema) module


### Output file format and schema

The data needs to be processed and stored in S3. Output data will be stored partitioned by date and hour in S3 and the file format should be parquet.

The output files should follow the schema:

| Field       |     Type     |  Partition key   |
| ----------- | ------------ | ---------------- |
| date        | string       |        Yes       |
| hour        | string       |        Yes       |
| event_id    | string       |                  |
| user_id     | string       |                  |
| postcode    | string       |                  |
| webpage     | string       |                  |
| timestamp   | timestamp    |                  |

Partition keys are saved only as part of the path following the `date=YYYY-MM-DD/hour=HH` format with hours padded to 2 digits.
Empty hours won't produce any files

### Source code modularity principles

Use of SOLID principles is encouraged to make different components decoupled and testable.


### Testing strategy

We require 3 different types of test: unit, integration tests and end to end tests.

This project needs to use JUnit 5 to run all the tests.
For integration and end to end tests, we will use Testcontainers to run localstack to simulate S3 and Apache Kafka images.

Unit test should run without any S3 or Kafka considerations
Integration test should run with either S3 (via localstack) or Kafka but never both in the same test.
End to end tests should run using Kafka, S3 (via localstack)


### Volume expectations

It is expected that the volumes of data are low, around 100k view events per day.

Data isn't expected to be skewed so no strategies should 

### How Flink should be setup

Flink needs to be setup using:
- End to end (Kafka -> S3), exactly once semantics so we don't end up with duplicated data
- We won't be using watermarks so all data should be processed regardless of it's time of arrival.
- While state isn't required, checkpointing is setup to ensure end to end (Kafka -> S3) exactly once semantics:
    - Unaligned Checkpoints to allow for more robust processing
    - State checkpointing stored using RocksDB to avoid losing state
    - Checkpoints will be stored in S3
    - Checkpointing interval set to 15 minutes
- This setup of checkpointing and parallelism will potentially generate a lot of files. The data we load into S3 is for landing purposes only and this should be compacted and moved to the appropriate place where it's consumed by batch processes. Compaction will happen at that point.
- To avoid idle partitions which will stall the whole processing, we will set idleness timeouts to 1 minute
- Kafka consumer should be setup to start reading from the earliest offset

### Dead Letter Queue

In cases where the payload can't be processed due to malformation, this event will be written to a DLQ folder in S3.
The schema would be as follows:

| Field                |     Type     |  Partition key   |
| -------------------- | ------------ | ---------------- |
| date                 | string       |        Yes       |
| payload              | string       |                  |
| kafka_offset         | long         |                  |
| kafka_partition      | integer      |                  |
| kafka_topic          | long         |                  |
| processing_timestamp | long         |                  |

Processing timestamp is the wall clock timestamp of when the data was processed by the ingestor and failed.
data will be derived from the processing timestamp

### Logging

Add logging on the following:
- Log when unsuccessful processing happened on events showing just the date, kafka topic, kafka partition and kafka offset
- Log after 10 events have been successfully processed. This will be in a per task basis to avoid coordination between tasks/nodes

### Configuration file

We should be allowed to change parameters in Flink, S3 buckets and folders via a configuration file. This configuration file should follow a yaml format

