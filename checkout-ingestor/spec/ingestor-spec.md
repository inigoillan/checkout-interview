# Checkout Ingestor

## Functional requirements

The checkout-ingestor module loads view events from the [event-schema](../../checkout-event-schema) module into the datalake to be consumed by batch processes.


## Technical Requirement

This module uses Flink and Java as the base to perform the load into S3


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
| user_id     | int          |                  |
| postcode    | string       |                  |
| webpage     | string       |                  |
| timestamp   | long         |                  |

date and hour are partition keys encoded in the path following the `date=YYYY-MM-DD/hour=HH` format with hours padded to 2 digits. They are derived from the timestamp (in UTC) field on the view event
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

Data isn't expected to be skewed so no strategies are required to handle hot topics


### How Flink should be setup

Flink needs to be setup using:
- End to end (Kafka -> S3), exactly once semantics so we don't end up with duplicated data
- We won't be using watermarks so all data should be processed regardless of its time of arrival.
- While state isn't required, checkpointing is setup to ensure end to end (Kafka -> S3) exactly once semantics:
    - Use of aligned checkpoints as backpressure is unlikely at this scale.
    - Checkpoints will be stored in S3 to ensure offsets are persisted
    - Checkpointing interval set to 15 minutes. This will make files only appear after 15 minutes so this parameter should be configurable in docker-compose (should be set to 30 seconds in this case)
- Sink parallelism will be set to 1 for this level of load.
- This setup of checkpointing and parallelism will potentially generate 4 files per hour. The data we load into S3 is for landing purposes only and this should be compacted and moved to the appropriate place where it's consumed by batch processes. Compaction will happen at that point.
- Kafka consumer should be setup to start reading from the earliest offset for the first time it runs


### Dead Letter Queue

In cases where the payload can't be parsed correctly into a ViewEvent object, this event will be written to a `dlq` folder on the same S3 bucket as parquet files.
The schema would be as follows:

| Field                |     Type     |  Partition key   |
| -------------------- | ------------ | ---------------- |
| date                 | string       |        Yes       |
| event                | string       |                  |
| kafka_offset         | long         |                  |
| kafka_partition      | integer      |                  |
| kafka_topic          | string       |                  |
| processing_timestamp | timestamp    |                  |

Processing timestamp is the wall clock timestamp in UTC of when the data was processed by the ingestor and failed.
date is the partition key encoded in the path following the `date=YYYY-MM-DD` format. It is derived from the processing timestamp


### Logging

Add logging on the following:
- Log when unsuccessful processing happened on events showing just the kafka topic, kafka partition and kafka offset. The payload won't be logged, this will only be available through the DLQ
- Log every 10 successfully processed events. This will be in a per task basis to avoid coordination between tasks/nodes


### Configuration file

We should be allowed to change parameters in Flink, S3 buckets and folders via a configuration file. This configuration file should follow a yaml format

