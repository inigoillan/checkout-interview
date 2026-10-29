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
- Kafka / Kafka Streams 4.3.2 (Docker: `apache/kafka:4.3.2`)
- Fory 1.7.7 (`fory-core`, `fory-json`)
- Jackson 2.22.4, SLF4J 2.0.20, Log4j2 2.26.1
- JUnit Jupiter 6.1.3, Testcontainers 2.0.5, AWS SDK v2 2.57.2 (S3), LocalStack `localstack/localstack:3.8` (pinned: `latest` now requires a Pro auth token)

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
`checkout.events.raw`, `checkout.events.sanitised`, `checkout.counts` via `kafka-init`.

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

Use `CheckoutEvents.fromJson(String)` / `fromJsonBytes(byte[])` /
`toJson(...)` / `toJsonBytes(...)` (Apache Fory JSON, thread-safe singleton).

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
checkout-count/               # Flink aggregator (scaffold) + Dockerfile
checkout-event-emitter/       # Kafka CLI producer (scaffold) + Dockerfile
```
