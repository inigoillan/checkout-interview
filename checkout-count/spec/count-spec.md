# Checkout Count

## Functional requirements

The checkout-count module aggregates view events from the [event-schema](../../checkout-event-schema) module to calculate 1 minute count over the following dimensions: postcode and time down to a 1 minute.

The latest data will be available to consumption, and it is the job of the consumer of the data to filter that data depending on whether they want to see incomplete hour data. In the case they want to filter the data out, they will need to filter the previous hour past 30 minutes after that hour has ended.

## Technical Requirement

This module uses Flink and Java as the base to perform the aggregations required.


### Input data

Data will be read from the output topic of the [qualitycheck](../../checkout-quality-check) module. This will come as a json view event


### Output file format and schema

The data needs to be processed and stored in S3. Output data will be stored partitioned by date and hour in S3 and the file format should be parquet.

The output files should follow the schema:

| Field       |     Type     |  Partition key   |
| ----------- | ------------ | ---------------- |
| date        | string       |        Yes       |
| hour        | string       |        Yes       |
| time_window | timestamp    |                  |
| postcode    | text         |                  |
| count       | integer      |                  |

time_window is the time in UTC when the window starts. time_window is, per the functional requirements, stored using minute granularity
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

Postcode data could be skewed so some postcodes will have more traffic/events than others.


### How Flink should be setup

Flink needs to be setup using:
- End to end (Kafka -> S3), exactly once semantics so we don't end up with duplicated data
- In order to manage out of order and late message arrival, we will use watermarks. The watermark should use a 30-minute bounded out-of-orderness on the view event timestamp. We won't allow any lateness as parquet files don't allow for upserts.  
- Checkpointing is setup with the following features:
  - Unaligned Checkpoints to allow for more robust processing
  - State checkpointing stored using RocksDB to avoid losing state
  - Checkpoints will be stored in S3
  - Checkpointing interval set to 5 minutes
- This setup of checkpointing will potentially generate a lot of files. We will consider creating a batch job to compact files in future versions.
- Given the expected amount of data and to avoid creating too many files, the sink parallelism will be set to 1
- Given the expected amount of data, we won't be considering any strategies to avoid overload of nodes due to hot partitions
- To avoid idle partitions which will stall the whole processing, we will set idleness timeouts to 1 minute
- Kafka consumer should be setup to start reading from the earliest offset. In any case the watermark will make most of the events go to the dlq but for testing is useful

### Late arrival folder format

In the cases where data arrives after the watermark window, we will store this data in a `dlq` folder within the s3 bucket partitioned by date. In those cases, we should store the upstream event (json view event), the timestamp stored in the Kafka topic and when we processed and discarded this event. This should be stored in parquet format.

The schema would be as follows:

| Field                |     Type     |  Partition key   |
| -------------------- | ------------ | ---------------- |
| date                 | string       |        Yes       |
| payload              | text         |                  |
| kafka_timestamp      | long         |                  |
| processing_timestamp | long         |                  |


### Configuration file

We should be allowed to change parameters in Flink, S3 buckets and folders via a configuration file. This configuration file should follow a yaml format

