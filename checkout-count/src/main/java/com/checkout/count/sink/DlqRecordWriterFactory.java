package com.checkout.count.sink;

import com.checkout.count.model.DlqRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class DlqRecordWriterFactory extends AvroRecordWriterFactory<DlqRecord> {

  static final String SCHEMA_JSON =
      """
      {
        "type": "record",
        "name": "DlqRecord",
        "namespace": "com.checkout.count",
        "fields": [
          {"name": "payload", "type": "string"},
          {"name": "kafka_timestamp", "type": "long"},
          {"name": "processing_timestamp", "type": "long"}
        ]
      }
      """;

  public DlqRecordWriterFactory() {
    super(SCHEMA_JSON);
  }

  @Override
  protected GenericRecord toRecord(DlqRecord record, Schema schema) {
    GenericRecord avro = new GenericData.Record(schema);
    avro.put("payload", record.payload);
    avro.put("kafka_timestamp", record.kafkaTimestamp);
    avro.put("processing_timestamp", record.processingTimestamp);
    return avro;
  }
}
