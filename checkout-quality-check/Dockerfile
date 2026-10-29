# checkout-quality-check runtime image.
# Build the staged libs first:
#   ./gradlew :checkout-quality-check:prepareDockerContext
# Then build from THIS module directory:
#   docker build -t checkout-quality-check:local -f Dockerfile .
FROM eclipse-temurin:17-jre-jammy AS runtime

WORKDIR /app

# Populated by Gradle task `prepareDockerContext` (build/docker-context/libs/*.jar).
COPY build/docker-context/libs /app/libs

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

ENTRYPOINT ["java", "-cp", "/app/libs/*", "com.checkout.qualitycheck.QualityCheckApp"]
