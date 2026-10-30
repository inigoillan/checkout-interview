package com.checkout.count.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.checkout.count.model.CountRow;
import com.checkout.count.model.DlqRecord;
import java.time.Instant;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class BucketAssignerTest {

  @Test
  void countRowsAreBucketedByWindowStartDateAndHourInUtc() {
    TimeWindowBucketAssigner assigner = new TimeWindowBucketAssigner();

    assertEquals(
        "date=2026-10-29/hour=09",
        assigner.getBucketId(row("2026-10-29T09:05:00Z"), null));
    assertEquals(
        "date=2026-10-29/hour=23",
        assigner.getBucketId(row("2026-10-29T23:59:00Z"), null));
    assertEquals(
        "date=2026-10-30/hour=00",
        assigner.getBucketId(row("2026-10-30T00:00:00Z"), null));
  }

  @Test
  void dlqRecordsAreBucketedByProcessingDate() {
    DlqBucketAssigner assigner = new DlqBucketAssigner();

    assertEquals(
        "date=2026-10-29",
        assigner.getBucketId(dlq("2026-10-29T23:59:59Z"), null));
  }

  private static CountRow row(String windowStart) {
    return new CountRow(Instant.parse(windowStart).toEpochMilli(), "SW1A", 1);
  }

  private static DlqRecord dlq(String processedAt) {
    return new DlqRecord("{}", 0L, Instant.parse(processedAt).toEpochMilli());
  }
}
