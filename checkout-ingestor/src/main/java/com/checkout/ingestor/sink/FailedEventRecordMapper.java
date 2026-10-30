package com.checkout.ingestor.sink;

import com.checkout.ingestor.parse.FailedEvent;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;

public final class FailedEventRecordMapper implements MapFunction<FailedEvent, GenericRecord> {

  @Override
  public GenericRecord map(FailedEvent failed) {
    GenericRecord record = new GenericData.Record(AvroSchemas.DLQ_RECORD);
    record.put("event", failed.payload());
    record.put("kafka_offset", failed.offset());
    record.put("kafka_partition", failed.partition());
    record.put("kafka_topic", failed.topic());
    record.put("processing_timestamp", failed.processingTimestamp());
    return record;
  }
}
