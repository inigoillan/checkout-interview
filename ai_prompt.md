# Prompts provided to AI tools

Models used: Deepseek v4.1 Flash
Harness: Opencode 2.2
Skills: No skills used




## Project generation prompt

Generate a multi-module Gradle java project using Kotlin DSL with the following specifications:
- Java and JVM should target version 17 as this is the recommended version of Flink
- The Gradle modules should allow for a step to generate a folder with the jar and all the dependencies that can then be copied into the docker container to run

The project should contain the following modules:
- **checkout-ingestor**. A module to ingest data into a data lake.
- **checkout-quality-check**. A QC gate to allow checkout-count to read sanitised data.
- **checkout-count**. An aggregator that aggregates data over a given set of dimensions.
- **checkout-event-emitter**. A CLI tool to send example data into a Kafka topic to be ingested by the first 2 modules.
- **checkout-event-schema**. A library that is shared across the above projects that exposes the schema model of the events.

The tech stack to use is going to be:
- Kafka
- Kafka Streams for the checkout-ingestor and checkout-quality-check modules
- Flink for the checkout-count module
- Apache Fory to deserialise json events
- SLF4j for logging using Log4j 2.x as the default mechanism
- Unit tests with JUnit Jupiter
    - Testcontainers for testing using localstack and Kafka

Use latest stable versions of all the dependencies, including the Gradle plugins and Gradle wrapper.

Your job is to create the scaffolding for the whole project but don't add any source code unless explicitly stated. Just do the following:

- Create the modules using Gladle. Add the mandatory dependencies as required.
  - The checkout-event-schema should have code to deserialise into a java record a json with the following schema:
    - **event_id**. string
    - **user_id**. int
    - **postcode**. string
    - **webpage**. string
    - **timestamp**. long. Expressed in seconds
- Add a hello world main on the modules that have executables (checkout-ingestor, checkout-quality-gate, checkout-count, checkout-event-emitter)
- Setup a local git repo
- Add a .gitignore file
- On the 4 first modules (checkout-ingestor, checkout-quality-gate, checkout-count and checkout-event-emitter), add a dockerfile that allows to run the module
- Add a docker compose file on the root of the project that allows to run the 3 different modules. This should also run a dockerised version of Kafka and local stack. 
- Create a README.md file on how to locally compile, run tests and run the docker image in each module.







