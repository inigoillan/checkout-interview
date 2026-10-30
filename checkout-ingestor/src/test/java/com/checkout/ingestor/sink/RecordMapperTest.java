package com.checkout.ingestor.sink;

import com.checkout.ingestor.parse.FailedEvent;
import com.checkout.schema.ViewEvent;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class RecordMapperTest {

  @Test
  void view_event_record_has_the_spec_columns_and_no_partition_columns() {
    GenericRecord record = new ViewEventRecordMapper()
        .map(new ViewEvent("evt-1", 42, "E1 6AN", "/checkout/confirm", 1718559600L));

    assertEquals("evt-1", record.get("event_id"));
    assertEquals(42, record.get("user_id"));
    assertEquals("E1 6AN", record.get("postcode"));
    assertEquals("/checkout/confirm", record.get("webpage"));
    assertEquals(1718559600L, record.get("timestamp"));
    assertNull(record.getSchema().getField("date"));
    assertNull(record.getSchema().getField("hour"));
  }

  @Test
  void dlq_record_has_the_spec_columns_and_excludes_date() {
    GenericRecord record = new FailedEventRecordMapper()
        .map(new FailedEvent("checkout.events.raw", 2, 7L, "{not json", 1718559600123L));

    assertEquals("{not json", record.get("event"));
    assertEquals(7L, record.get("kafka_offset"));
    assertEquals(2, record.get("kafka_partition"));
    assertEquals("checkout.events.raw", record.get("kafka_topic"));
    assertEquals(1718559600123L, record.get("processing_timestamp"));
    assertNull(record.getSchema().getField("date"));
  }
}
