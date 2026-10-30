package com.checkout.ingestor.support;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.localstack.LocalStackContainer;

public final class Containers {

  public static final LocalStackContainer LOCALSTACK = new LocalStackContainer("localstack/localstack:4.14");
  public static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

  static {
    LOCALSTACK.start();
    KAFKA.start();
  }

  private Containers() {}
}
