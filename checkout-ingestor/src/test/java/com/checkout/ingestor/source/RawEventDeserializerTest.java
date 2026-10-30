package com.checkout.ingestor.source;

import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class RawEventDeserializerTest {

  @Test
  void keeps_topic_partition_offset_and_payload() {
    List<RawEvent> out = deserialize(new ConsumerRecord<>(
        "checkout.events.raw", 2, 7L, null, "{\"event_id\":\"evt-1\"}".getBytes(StandardCharsets.UTF_8)));

    assertEquals(List.of(new RawEvent("checkout.events.raw", 2, 7L, "{\"event_id\":\"evt-1\"}")), out);
  }

  @Test
  void treats_a_null_value_as_an_empty_payload() {
    List<RawEvent> out = deserialize(new ConsumerRecord<>("checkout.events.raw", 0, 0L, null, null));

    assertEquals(List.of(new RawEvent("checkout.events.raw", 0, 0L, "")), out);
  }

  private static List<RawEvent> deserialize(ConsumerRecord<byte[], byte[]> record) {
    List<RawEvent> out = new ArrayList<>();
    new RawEventDeserializer().deserialize(record, new Collector<RawEvent>() {
      @Override
      public void collect(RawEvent value) {
        out.add(value);
      }

      @Override
      public void close() {}
    });
    return out;
  }
}
