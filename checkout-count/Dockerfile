# checkout-count (Flink job) runtime image.
# Build the staged libs first:
#   ./gradlew :checkout-count:prepareDockerContext
# Then build from THIS module directory:
#   docker build -t checkout-count:local -f Dockerfile .
FROM eclipse-temurin:17-jre-jammy AS runtime

WORKDIR /app

# Populated by Gradle task `prepareDockerContext` (build/docker-context/libs/*.jar).
COPY build/docker-context/libs /app/libs

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV AWS_ENDPOINT_URL=http://localstack:4566

# Runs the Flink job in local/embedded mode. For a real cluster, submit
# the job jar to a JobManager instead (see module README notes).
ENTRYPOINT ["java", "-cp", "/app/libs/*", "com.checkout.count.CountJob"]
