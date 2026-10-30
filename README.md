# Checkout Interview — Streaming Scaffolding

Multi-module Gradle (Kotlin DSL) project, Java/JVM 17.

Modules:

| Module | Type | Stack |
|---|---|---|
| `checkout-event-schema` | shared library | Apache Fory JSON (`fory-json`), Jackson, SLF4J + Log4j2 |
| `checkout-ingestor` | Flink job, Kafka → S3 data lake | Flink DataStream, FileSink, Flink Kafka connector, Native S3 FS, AWS SDK S3 |
| `checkout-quality-check` | Kafka Streams QC gate | Kafka Streams, Kafka clients, Fory JSON |
| `checkout-count` | Flink aggregator | Flink DataStream, Flink Kafka connector, AWS SDK S3, Fory JSON |
| `checkout-event-emitter` | CLI → Kafka topic | Kafka clients, Fory JSON |

Pinned latest-stable versions (Oct 2026):

- Gradle 9.8.2, Java 17 (toolchain + `release = 17`)
- Flink 2.3.0, Flink Kafka connector 4.0.0-2.0
- Kafka / Kafka Streams 4.4.0 (Docker: `apache/kafka:4.4.0`)
- Fory 1.7.7 (`fory-core`, `fory-json`)
- Jackson 2.22.4, SLF4J 2.0.20, Log4j2 2.26.1
- JUnit Jupiter 6.1.3, Testcontainers 2.0.5, AWS SDK v2 2.57.2 (S3), LocalStack `localstack/localstack:4.14` (pinned: newer tags require a LocalStack auth token)

`checkout-event-schema` holds the shared record + Fory deserialiser.
The other four modules each ship a hello-world entrypoint (SLF4J + stdout),
with mandatory dependencies, a `prepareDockerContext` staging task, and a
`Dockerfile`.

## Prerequisites

- JDK 17+ to run Gradle (project toolchains to Java 17; Gradle auto-provisions it if missing).
- Docker + Docker Compose v2 (for Testcontainers and for `docker-compose.yml`).
- GNU `make` (all workflows below go through the `Makefile`; raw `./gradlew` /
  `docker compose` equivalents are listed after each `make` target).
- No global Gradle needed: the `Makefile` uses `./gradlew`.

## Compile

```bash
make build          # ./gradlew build -x test
```

Single module (raw Gradle):

```bash
./gradlew :checkout-event-schema:build -x test
./gradlew :checkout-ingestor:build -x test
./gradlew :checkout-quality-check:build -x test
./gradlew :checkout-count:build -x test
./gradlew :checkout-event-emitter:build -x test
```

## Run tests

Testcontainers (Kafka, LocalStack) needs a running Docker daemon.

```bash
make test           # ./gradlew test
```

Per module (raw Gradle):

```bash
./gradlew :checkout-event-schema:test
./gradlew :checkout-ingestor:test
./gradlew :checkout-quality-check:test
./gradlew :checkout-count:test
./gradlew :checkout-event-emitter:test
```

`checkout-event-schema` ships two real tests (`ViewEventTest`):
snake_case JSON deserialisation and a Fory round-trip.

## Stage jar + dependencies (for Docker)

Each runnable module has a `prepareDockerContext` task that creates:

```
<module>/build/docker-context/libs/*.jar   # app jar + every runtime dep
```

```bash
make stage          # stages all four runnable modules
# raw equivalent:
# ./gradlew :checkout-ingestor:prepareDockerContext \
#           :checkout-quality-check:prepareDockerContext \
#           :checkout-count:prepareDockerContext \
#           :checkout-event-emitter:prepareDockerContext
ls checkout-ingestor/build/docker-context/libs | head
```

Dockerfiles (`checkout-*/Dockerfile`, base `amazoncorretto:17`) are single-stage
images that `COPY build/docker-context/libs /app/libs` and run
`java -cp "/app/libs/*" <MainClass>`.

## Run a module Docker image

```bash
make docker-build   # make stage + docker compose build (compiles everything)

# per-module standalone image, e.g. ingestor:
./gradlew :checkout-ingestor:prepareDockerContext
# (context MUST be the module dir, Dockerfile expects build/ inside it)
docker build -t checkout-ingestor:local -f checkout-ingestor/Dockerfile checkout-ingestor

# run (join the compose network to reach kafka/localstack, or override env)
docker run --rm \
  --network checkout-interview_default \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e AWS_ENDPOINT_URL=http://localstack:4566 \
  checkout-ingestor:local
```

Per module:

| Module | Main class | Build + run |
|---|---|---|
| `checkout-ingestor` | `com.checkout.ingestor.IngestorApp` | `./gradlew :checkout-ingestor:prepareDockerContext` then `docker build -t checkout-ingestor:local -f checkout-ingestor/Dockerfile checkout-ingestor` |
| `checkout-quality-check` | `com.checkout.qualitycheck.QualityCheckApp` | `./gradlew :checkout-quality-check:prepareDockerContext` then `docker build -t checkout-quality-check:local -f checkout-quality-check/Dockerfile checkout-quality-check` |
| `checkout-count` | `com.checkout.count.CountJob` | `./gradlew :checkout-count:prepareDockerContext` then `docker build -t checkout-count:local -f checkout-count/Dockerfile checkout-count` |
| `checkout-event-emitter` | `com.checkout.emitter.EventEmitterApp` | `./gradlew :checkout-event-emitter:prepareDockerContext` then `docker build -t checkout-event-emitter:local -f checkout-event-emitter/Dockerfile checkout-event-emitter`; run e.g. `docker run --rm --network checkout-interview_default checkout-event-emitter:local --topic checkout.events.raw --count 100` |

Direct Gradle run (without Docker):

```bash
./gradlew :checkout-ingestor:run
./gradlew :checkout-quality-check:run
./gradlew :checkout-count:run
./gradlew :checkout-event-emitter:run --args="--topic checkout.events.raw --count 10"
```

Each prints `Hello world from <module>` via SLF4J/Log4j2 and stdout.

## Run everything with Docker Compose

`docker-compose.yml` (root) starts Kafka (KRaft), LocalStack (S3),
the three long-running services (`checkout-ingestor`,
`checkout-quality-check`, `checkout-count`), and the one-shot
`checkout-event-emitter`. It also creates topics
`checkout.events.raw`, `checkout.events.sanitised`, `checkout.qualitycheck.dlq` via `kafka-init`.

```bash
make up             # stage + docker compose up --build (foreground, compiles everything)
make up-d           # detached variant
make logs           # follow logs
make down           # docker compose down -v
make ps             # container status

# raw equivalents:
# docker compose build
# docker compose up
# docker compose up --build -d
# docker compose logs -f
# docker compose down -v
```

Kafka exposed on `localhost:29092` (host) / `kafka:9092` (compose network).
LocalStack S3 on `http://localhost:4566`.

Send test traffic with the emitter (runs unconditionally as a one-shot service,
default args `--topic checkout.events.raw --count 100`):

```bash
make emitter                              # default args
make emitter ARGS="--topic checkout.events.raw --count 10"   # custom args

# raw equivalent:
# docker compose run --rm checkout-event-emitter --topic checkout.events.raw --count 10
```

Or standalone Docker (host network example):

```bash
./gradlew :checkout-event-emitter:prepareDockerContext
docker build -t checkout-event-emitter:local -f checkout-event-emitter/Dockerfile checkout-event-emitter
docker run --rm --network checkout-interview_default \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  checkout-event-emitter:local --topic checkout.events.raw --count 100
```

Or without Docker:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 ./gradlew :checkout-event-emitter:run --args="--topic checkout.events.raw --count 10"
```

## Event schema

`ViewEvent` (`com.checkout.schema`, Java record):

- `event_id: string`
- `user_id: int`
- `postcode: string`
- `webpage: string`
- `timestamp: long` (seconds)

Use `ViewEvents.fromJson(String)` / `ViewEvents.toJson(ViewEvent)` (Apache Fory JSON,
thread-safe shared instance).

## checkout-count

Reads view events from `checkout.events.sanitised`, counts them per outward postcode in one-minute
tumbling windows, and writes Parquet files to S3. Events that arrive after their window has
closed go to a `dlq` folder.

Configuration lives in `checkout-count/src/main/resources/checkout-count.yaml`. Set
`CHECKOUT_COUNT_CONFIG` to a file path to override it. Compose mounts
`checkout-count/config/checkout-count.compose.yaml` for the containerised job.

| Key | Default | Purpose |
|---|---|---|
| `kafka.bootstrap-servers` | `localhost:29092` | Kafka bootstrap servers |
| `kafka.input-topic` | `checkout.events.sanitised` | Source topic |
| `kafka.start-offset` | `earliest` | `earliest`, `latest`, or `committed` |
| `s3.region` | `eu-west-1` | AWS region |
| `s3.endpoint` | blank | S3 endpoint; `AWS_ENDPOINT_URL` is used when blank |
| `s3.output` | `checkout-lake-raw` / `counts` | Bucket and prefix for counts |
| `s3.dlq` | `checkout-lake-raw` / `dlq` | Bucket and prefix for late events |
| `s3.checkpoints.location` | `s3://checkout-checkpoints/checkout-count` | Checkpoint storage |
| `flink.parallelism.sink` | `1` | Parallelism of both sinks |
| `flink.watermark.bound` | `30m` | Out-of-orderness bound on event time |
| `flink.watermark.idleness` | `1m` | Idle partitions stop holding back the watermark |
| `flink.checkpointing.interval` | `5m` | Checkpoint interval |
| `flink.checkpointing.unaligned` | `true` | Unaligned checkpoints |

Output layout:

```
s3://checkout-lake-raw/counts/date=YYYY-MM-DD/hour=HH/part-*
s3://checkout-lake-raw/dlq/date=YYYY-MM-DD/part-*
```

Count rows have `time_window` (UTC window start, minute granularity), `postcode` (the outward
code, e.g. `SW1A` for `SW1A 1AA`), and `count`. Dead-letter rows have `payload` (the original JSON), `kafka_timestamp`, and
`processing_timestamp`, both in epoch milliseconds. Partition values appear only in the path.
Hours with no events produce no files.

Consumers should treat an hour as complete 30 minutes after it ends, measured in event time,
so `hour=10` is final once events timestamped after 10:30 have been seen. Filter out the
current hour and any hour that has not reached that point.

Run the tests (Docker is needed for everything except the unit tests):

```bash
./gradlew :checkout-count:test
```

The tests are tagged `unit`, `integration-kafka`, `integration-s3`, and `e2e`.

## checkout-ingestor

Reads raw view events from `checkout.events.raw` and lands them in S3 as Parquet, with no
validation beyond parsing. Events that can't be parsed into a `ViewEvent` go to a DLQ.
Delivery is exactly-once, using checkpoints stored in S3.

Configuration lives in `checkout-ingestor/src/main/resources/checkout-ingestor.yaml`. Set
`CHECKOUT_INGESTOR_CONFIG` to a file path to override it. Compose mounts
`checkout-ingestor/config/checkout-ingestor.compose.yaml` (30-second checkpoints) for the
containerised job. `KAFKA_BOOTSTRAP_SERVERS`, `AWS_ENDPOINT_URL`, `AWS_REGION`,
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` override the matching settings.

Output layout:

```
s3://checkout-lake-raw/landing/date=YYYY-MM-DD/hour=HH/part-*.parquet
s3://checkout-lake-raw/dlq/date=YYYY-MM-DD/part-*.parquet
```

Landing files contain `event_id`, `user_id`, `postcode`, `webpage` and `timestamp` (epoch
seconds). Partition values come from the path only. DLQ files contain `event`, `kafka_offset`,
`kafka_partition`, `kafka_topic` and `processing_timestamp`. The DLQ `date` comes from the path
only, and it is the UTC date of processing. Hours with no events produce no files.

Run the tests (Docker is needed for everything except the unit tests):

```bash
./gradlew :checkout-ingestor:test
```

## Layout

```
Makefile
settings.gradle.kts
build.gradle.kts
gradle/libs.versions.toml
gradle.properties
docker-compose.yml
checkout-event-schema/        # shared record + Fory serde + tests
checkout-ingestor/            # Flink FileSink job, Kafka -> S3 lake + Dockerfile
checkout-quality-check/       # Kafka Streams QC gate (scaffold) + Dockerfile
checkout-count/               # Flink aggregator (Kafka -> S3 Parquet) + config + Dockerfile
checkout-event-emitter/       # Kafka CLI producer (--topic, --count) + Dockerfile
```
