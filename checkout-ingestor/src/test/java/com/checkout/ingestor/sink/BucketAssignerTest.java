package com.checkout.ingestor.sink;

import com.checkout.ingestor.parse.FailedEvent;
import com.checkout.schema.ViewEvent;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class BucketAssignerTest {

  private final HourBucketAssigner hourAssigner = new HourBucketAssigner();
  private final DateBucketAssigner dateAssigner = new DateBucketAssigner();

  @Test
  void assigns_the_utc_date_and_hour_of_the_event_timestamp() {
    GenericRecord record = view("2026-10-30T09:59:59Z");

    assertEquals("date=2026-10-30/hour=09", hourAssigner.getBucketId(record, null));
  }

  @Test
  void rolls_over_to_the_next_hour_on_the_boundary() {
    GenericRecord record = view("2026-10-30T10:00:00Z");

    assertEquals("date=2026-10-30/hour=10", hourAssigner.getBucketId(record, null));
  }

  @Test
  void pads_single_digit_hours() {
    GenericRecord record = view("2026-01-02T03:00:00Z");

    assertEquals("date=2026-01-02/hour=03", hourAssigner.getBucketId(record, null));
  }

  @Test
  void dlq_bucket_is_the_utc_date_of_the_processing_timestamp() {
    GenericRecord record = failed(Instant.parse("2026-10-30T23:59:59Z").toEpochMilli());

    assertEquals("date=2026-10-30", dateAssigner.getBucketId(record, null));
  }

  private static GenericRecord view(String timestamp) {
    long seconds = Instant.parse(timestamp).getEpochSecond();
    return new ViewEventRecordMapper().map(new ViewEvent("evt", 1, "E1 6AN", "/x", seconds));
  }

  private static GenericRecord failed(long processingTimestamp) {
    return new FailedEventRecordMapper().map(new FailedEvent("topic", 0, 1L, "payload", processingTimestamp));
  }
}
