package com.checkout.ingestor.source;

import com.checkout.ingestor.config.IngestorConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public final class KafkaSourceFactory {

  private KafkaSourceFactory() {}

  public static KafkaSourceBuilder<RawEvent> builder(IngestorConfig config) {
    return KafkaSource.<RawEvent>builder()
        .setBootstrapServers(config.kafka().bootstrapServers())
        .setTopics(config.kafka().inputTopic())
        .setGroupId(config.kafka().groupId())
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(new RawEventDeserializer());
  }
}
