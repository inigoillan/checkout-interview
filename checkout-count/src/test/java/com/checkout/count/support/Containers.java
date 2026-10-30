package com.checkout.count.support;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/** Shared Kafka and LocalStack containers, started once per test JVM. */
public final class Containers {

  private static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0"));

  private static final LocalStackContainer LOCALSTACK =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.14"));

  static {
    KAFKA.start();
    LOCALSTACK.start();
  }

  private Containers() {}

  public static String bootstrapServers() {
    return KAFKA.getBootstrapServers();
  }

  public static String s3Endpoint() {
    return LOCALSTACK.getEndpoint().toString();
  }
}
