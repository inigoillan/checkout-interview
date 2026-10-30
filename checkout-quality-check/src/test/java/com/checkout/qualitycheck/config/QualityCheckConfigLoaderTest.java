package com.checkout.qualitycheck.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class QualityCheckConfigLoaderTest {

  @Test
  void loadsDefaultClasspathConfig() {
    QualityCheckConfig config = QualityCheckConfigLoader.fromClasspath();

    assertEquals("checkout.events.raw", config.kafka().inputTopic());
    assertEquals("checkout.events.sanitised", config.kafka().outputTopic());
    assertEquals("checkout.qualitycheck.dlq", config.kafka().dlqTopic());
    assertTrue(config.checks().eventId().enabled());
    assertTrue(config.checks().postcode().enabled());
    assertTrue(config.checks().webpage().enabled());
    assertTrue(config.checks().timestamp().enabled());
    assertEquals(86_400L, config.checks().timestamp().maxAgeSeconds());
    assertEquals(600L, config.checks().timestamp().maxFutureSeconds());
  }

  @Test
  void streamsPropertiesUseExactlyOnceByDefault() {
    QualityCheckConfig config = QualityCheckConfigLoader.fromClasspath();

    assertEquals(
        "exactly_once_v2",
        config.kafka().streamsProperties().getProperty("processing.guarantee"));
  }
}
