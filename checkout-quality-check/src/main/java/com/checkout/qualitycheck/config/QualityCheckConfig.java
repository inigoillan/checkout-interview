package com.checkout.qualitycheck.config;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;

public record QualityCheckConfig(KafkaSettings kafka, ChecksSettings checks) {

  public QualityCheckConfig withBootstrapServers(String bootstrapServers) {
    return new QualityCheckConfig(kafka.withBootstrapServers(bootstrapServers), checks);
  }

  public record KafkaSettings(
      String bootstrapServers,
      String applicationId,
      String inputTopic,
      String outputTopic,
      String dlqTopic,
      Map<String, String> streams) {

    public KafkaSettings withBootstrapServers(String bootstrapServers) {
      return new KafkaSettings(
          bootstrapServers, applicationId, inputTopic, outputTopic, dlqTopic, streams);
    }

    public Properties streamsProperties() {
      Properties properties = new Properties();
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
      if (streams != null) {
        properties.putAll(streams);
      }
      return properties;
    }
  }

  public record ChecksSettings(
      Toggle eventId, Toggle postcode, Toggle webpage, TimestampSettings timestamp) {}

  public record Toggle(boolean enabled) {}

  public record TimestampSettings(boolean enabled, long maxAgeSeconds, long maxFutureSeconds) {}
}
