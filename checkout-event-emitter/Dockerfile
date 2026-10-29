# checkout-event-emitter CLI runtime image.
# Build the staged libs first:
#   ./gradlew :checkout-event-emitter:prepareDockerContext
# Then build from THIS module directory:
#   docker build -t checkout-event-emitter:local -f Dockerfile .
FROM amazoncorretto:17 AS runtime

WORKDIR /app

# Populated by Gradle task `prepareDockerContext` (build/docker-context/libs/*.jar).
COPY build/docker-context/libs /app/libs

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Example: docker run --rm --network checkout-interview_default checkout-event-emitter:local --topic checkout.events.raw --count 100
ENTRYPOINT ["java", "-cp", "/app/libs/*", "com.checkout.emitter.EventEmitterApp"]
