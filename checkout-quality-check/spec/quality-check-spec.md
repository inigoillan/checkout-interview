# Checkout Quality Gate

## Functional requirements

The checkout-quality-check module sanitizes the events from upstream. For now, it only sanitizes view events from the [event-schema](../../checkout-event-schema) module. The quality check module treats events as immutable so no changes to the fields. The quality check is performed individually over multiple fields:


### ViewEvents

#### event_id checks

1. Checks the field isn't null or empty

#### postcode checks

1. Checks the postcode is a valid UK postcode in uppercase. Both the spaced form (SW1A 1AA) and the unspaced form (SW1A1AA) are allowed.

#### webpage checks

1. Checks that this field contains a well formed URL. This should follow the URI standard in java.net.URI. Only http and https are allowed and when the scheme is missing, https is assumed.

#### timestamp checks

1. Checks that the event timestamp happened less than 1 day ago or 10 minutes in advance from current date and time
2. Cover and check the timestamp is in seconds (Point 1. would cover this)

## Technical Requirement

This module uses Kafka streams and Java as the base to perform the quality checks specified above


### Input data

Data is read from the `checkout.events.raw` Kafka topic. This will come as a json view event


### Output data

Data is written into the `checkout.events.sanitised` Kafka topic on the same JSON format as the upstream data, with the same record key as the upstream record


### Source code modularity principles

Use of SOLID principles is encouraged to make different components decoupled and testable.


### Testing strategy

We require 2 different types of test: unit and end to end tests.

This project uses JUnit 5 to run all the tests.
For end to end tests, we will use Testcontainers to run Apache Kafka docker images.

Unit test should run without any Kafka instances
End to end tests should run using Kafka from Testcontainers


### Volume expectations

It is expected that the volumes of data are low, around 100k view events per day.

We don't expect any skew in the data so no hot partition should be considered


### How Kafka Streams should be setup

Kafka Streams needs to be setup using:
- End to end (Kafka -> Kafka), exactly once semantics using atomic writes to commit both data and offsets to Kafka
- Kafka offsets should be set to earliest, to process all the data in the topic



### Dead Letter Queue

- Data that doesn't meet the quality gate, will be sent into a DLQ topic (`checkout.qualitycheck.dlq`) for further analysis. This will contain the json event payload as well as the set of filters that failed to pass and the reason if there's any

The schema then would be:

```
{ "event": "{...}",
 "failures": [{ "check": "postcode", "reason": "Malformed postcode" }]
}
```

The key used on the DLQ is the same as the `event_id`. In case of malformed JSON, or a missing or empty `event_id`, a randomly generated UUID is used

### Configuration file

This configuration file should follow a yaml format. It should allow to change parameters in Kafka Streams and enable and disable checks. 

