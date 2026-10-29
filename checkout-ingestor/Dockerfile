# checkout-ingestor runtime image.
# Build the staged libs first:
#   ./gradlew :checkout-ingestor:prepareDockerContext
# Then build from THIS module directory:
#   docker build -t checkout-ingestor:local -f Dockerfile .
FROM eclipse-temurin:17-jre-jammy AS runtime

WORKDIR /app

# Populated by Gradle task `prepareDockerContext` (build/docker-context/libs/*.jar).
COPY build/docker-context/libs /app/libs

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV AWS_ENDPOINT_URL=http://localstack:4566

ENTRYPOINT ["java", "-cp", "/app/libs/*", "com.checkout.ingestor.IngestorApp"]
