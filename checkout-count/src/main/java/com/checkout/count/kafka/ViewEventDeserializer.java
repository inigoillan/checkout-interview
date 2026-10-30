package com.checkout.count.kafka;

import com.checkout.count.model.TimedViewEvent;
import com.checkout.schema.ViewEvent;
import com.checkout.schema.ViewEvents;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ViewEventDeserializer implements KafkaRecordDeserializationSchema<TimedViewEvent> {

  private static final long MILLIS_PER_SECOND = 1000L;

  @Override
  public void deserialize(
      ConsumerRecord<byte[], byte[]> record, Collector<TimedViewEvent> out) {
    String payload = new String(record.value(), StandardCharsets.UTF_8);
    ViewEvent event = ViewEvents.fromJson(payload);
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
}
