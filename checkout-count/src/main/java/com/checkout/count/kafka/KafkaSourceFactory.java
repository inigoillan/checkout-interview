package com.checkout.count.kafka;

import com.checkout.count.config.CountConfig.KafkaSettings;
import com.checkout.count.model.TimedViewEvent;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public final class KafkaSourceFactory {

  public static final String GROUP_ID = "checkout-count";

  private KafkaSourceFactory() {}

  public static KafkaSource<TimedViewEvent> create(KafkaSettings settings) {
    return KafkaSource.<TimedViewEvent>builder()
        .setBootstrapServers(settings.bootstrapServers())
        .setTopics(settings.inputTopic())
        .setGroupId(GROUP_ID)
        .setStartingOffsets(startingOffsets(settings.startOffset()))
        .setDeserializer(new ViewEventDeserializer())
        .build();
  }

  static OffsetsInitializer startingOffsets(String startOffset) {
    return switch (startOffset) {
      case "earliest" -> OffsetsInitializer.earliest();
      case "latest" -> OffsetsInitializer.latest();
      case "committed" -> OffsetsInitializer.committedOffsets();
      default -> throw new IllegalArgumentException("Unsupported start-offset: " + startOffset);
    };
  }
}
