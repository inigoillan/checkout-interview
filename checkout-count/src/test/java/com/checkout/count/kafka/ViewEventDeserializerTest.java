package com.checkout.count.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.count.model.TimedViewEvent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("unit")
class ViewEventDeserializerTest {

  private static final long KAFKA_TIMESTAMP = 1718559601000L;

  private final ViewEventDeserializer deserializer = new ViewEventDeserializer();

  @Test
  void emitsValidEventWithFullPostcodeAndEventTimeInMillis() {
    List<TimedViewEvent> out =
        deserialize(
            "{\"event_id\":\"evt-1\",\"user_id\":42,\"postcode\":\"SW1A 1AA\","
                + "\"webpage\":\"/checkout/confirm\",\"timestamp\":1718559600}");

    assertEquals(1, out.size());
    assertEquals("SW1A 1AA", out.get(0).postcode);
    assertEquals(1718559600000L, out.get(0).eventTimeMillis);
    assertEquals(KAFKA_TIMESTAMP, out.get(0).kafkaTimestamp);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "not json",
        "{\"event_id\":",
        "null",
      })
  void skipsRecordsThatAreNotViewEventJson(String json) {
    assertTrue(deserialize(json).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(strings = {"sw1a 1aa", "SW1A", "1AA SW1", "EC1A 1BB1"})
  void skipsEventsWithPostcodesThatAreNotFullUkPostcodes(String postcode) {
    String json =
        "{\"event_id\":\"evt-1\",\"user_id\":42,\"postcode\":\""
            + postcode
            + "\",\"webpage\":\"/checkout/confirm\",\"timestamp\":1718559600}";

    assertTrue(deserialize(json).isEmpty());
  }

  @Test
  void skipsEventsWithoutAPostcode() {
    String json =
        "{\"event_id\":\"evt-1\",\"user_id\":42,\"webpage\":\"/checkout/confirm\","
            + "\"timestamp\":1718559600}";

    assertTrue(deserialize(json).isEmpty());
  }

  @Test
  void skipsRecordsWithoutAValue() {
    assertTrue(deserialize(record(null)).isEmpty());
  }

  private List<TimedViewEvent> deserialize(String json) {
    return deserialize(record(json));
  }

  private List<TimedViewEvent> deserialize(ConsumerRecord<byte[], byte[]> record) {
    List<TimedViewEvent> out = new ArrayList<>();
    deserializer.deserialize(record, new ListCollector(out));
    return out;
  }

  private static ConsumerRecord<byte[], byte[]> record(String json) {
    return new ConsumerRecord<>(
        "checkout.events.sanitised",
        0,
        7L,
        KAFKA_TIMESTAMP,
        TimestampType.CREATE_TIME,
        0,
        0,
        null,
        json == null ? null : json.getBytes(StandardCharsets.UTF_8),
        new RecordHeaders(),
        Optional.empty());
  }

  private static final class ListCollector implements Collector<TimedViewEvent> {

    private final List<TimedViewEvent> out;

    ListCollector(List<TimedViewEvent> out) {
      this.out = out;
    }

    @Override
    public void collect(TimedViewEvent event) {
      out.add(event);
    }

    @Override
    public void close() {}
  }
}
