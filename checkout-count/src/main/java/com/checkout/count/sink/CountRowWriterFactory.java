package com.checkout.count.sink;

import com.checkout.count.model.CountRow;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class CountRowWriterFactory extends AvroRecordWriterFactory<CountRow> {

  static final String SCHEMA_JSON =
      """
      {
        "type": "record",
        "name": "CountRow",
        "namespace": "com.checkout.count",
        "fields": [
          {"name": "time_window", "type": {"type": "long", "logicalType": "timestamp-millis"}},
          {"name": "postcode", "type": "string"},
          {"name": "count", "type": "int"}
        ]
      }
      """;

  public CountRowWriterFactory() {
    super(SCHEMA_JSON);
  }

  @Override
  protected GenericRecord toRecord(CountRow row, Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("time_window", row.timeWindow);
    record.put("postcode", row.postcode);
    record.put("count", row.count);
    return record;
  }
}
