package com.checkout.count.kafka;

import com.checkout.count.model.TimedViewEvent;
import com.checkout.count.pipeline.OutwardPostcode;
import com.checkout.schema.ViewEvent;
import com.checkout.schema.ViewEvents;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewEventDeserializer implements KafkaRecordDeserializationSchema<TimedViewEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ViewEventDeserializer.class);
  private static final long MILLIS_PER_SECOND = 1000L;

  @Override
  public void deserialize(
      ConsumerRecord<byte[], byte[]> record, Collector<TimedViewEvent> out) {
    String payload;
    ViewEvent event;
    try {
      payload = new String(record.value(), StandardCharsets.UTF_8);
      event = Objects.requireNonNull(ViewEvents.fromJson(payload));
    } catch (RuntimeException e) {
      reject(record, "malformed_json", e);
      return;
    }
    try {
      OutwardPostcode.of(event.postcode());
    } catch (RuntimeException e) {
      reject(record, "invalid_postcode", e);
      return;
    }
    out.collect(
        new TimedViewEvent(
            payload,
            event.postcode(),
            event.timestamp() * MILLIS_PER_SECOND,
            record.timestamp()));
  }

  @Override
  public TypeInformation<TimedViewEvent> getProducedType() {
    return TypeInformation.of(TimedViewEvent.class);
  }

  private static void reject(
      ConsumerRecord<byte[], byte[]> record, String reason, RuntimeException cause) {
    LOG.warn(
        "event.rejected reason={} error={} topic={} partition={} offset={}",
        reason,
        cause.getClass().getSimpleName(),
        record.topic(),
        record.partition(),
        record.offset());
  }
}
