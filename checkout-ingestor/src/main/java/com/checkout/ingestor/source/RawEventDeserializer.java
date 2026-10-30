package com.checkout.ingestor.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public final class RawEventDeserializer implements KafkaRecordDeserializationSchema<RawEvent> {

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawEvent> out) {
    String payload = record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);
    out.collect(new RawEvent(record.topic(), record.partition(), record.offset(), payload));
  }

  @Override
  public TypeInformation<RawEvent> getProducedType() {
    return TypeInformation.of(RawEvent.class);
  }
}
