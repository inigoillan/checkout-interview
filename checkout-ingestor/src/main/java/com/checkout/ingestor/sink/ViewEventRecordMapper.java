package com.checkout.ingestor.sink;

import com.checkout.schema.ViewEvent;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;

public final class ViewEventRecordMapper implements MapFunction<ViewEvent, GenericRecord> {

  @Override
  public GenericRecord map(ViewEvent event) {
    GenericRecord record = new GenericData.Record(AvroSchemas.VIEW_EVENT);
    record.put("event_id", event.eventId());
    record.put("user_id", event.userId());
    record.put("postcode", event.postcode());
    record.put("webpage", event.webpage());
    record.put("timestamp", event.timestamp());
    return record;
  }
}
