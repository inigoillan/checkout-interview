package com.checkout.count.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.count.CountJob;
import com.checkout.count.config.CountConfig;
import com.checkout.count.model.CountRow;
import com.checkout.count.model.DlqRecord;
import com.checkout.count.support.Containers;
import com.checkout.count.support.S3Support;
import com.checkout.count.support.TestConfigs;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3Client;

/** S3 only: records are written directly to LocalStack, without Kafka. */
@Tag("integration-s3")
class S3SinkIntegrationTest {

  private static final String BUCKET = "count-s3-it";
  private static S3Client s3;

  @TempDir static Path tmp;

  @BeforeAll
  static void createBucket() {
    s3 = S3Support.client();
    S3Support.createBucket(s3, BUCKET);
  }

  @Test
  void writesCountsAndDlqUnderHourAndDatePartitions() throws Exception {
    CountConfig config =
        TestConfigs.config(
            "unused", "unused", "earliest", Containers.s3Endpoint(), BUCKET, "1s", "1m");

    StreamExecutionEnvironment env = CountJob.createEnvironment(config);
    DataStream<CountRow> rows =
        env.fromData(
            List.of(
                new CountRow(millis("2026-10-29T10:00:00Z"), "SW1A", 3),
                new CountRow(millis("2026-10-29T10:01:00Z"), "SW1A", 2),
                new CountRow(millis("2026-10-29T11:05:00Z"), "EC1A", 1)),
            TypeInformation.of(CountRow.class));
    DataStream<DlqRecord> dlq =
        env.fromData(
            List.of(
                new DlqRecord(
                    "{\"event_id\":\"late-1\"}",
                    millis("2026-10-29T10:00:20Z"),
                    millis("2026-10-29T12:00:00Z"))),
            TypeInformation.of(DlqRecord.class));

    new S3CountSink(config.s3().output(), 1).write(rows);
    new S3DlqSink(config.s3().dlq(), 1).write(dlq);
    env.execute("s3-sink-it");

    List<String> countKeys = S3Support.keys(s3, BUCKET, "counts/");
    assertTrue(countKeys.stream().anyMatch(key -> key.startsWith("counts/date=2026-10-29/hour=10/")));
    assertTrue(countKeys.stream().anyMatch(key -> key.startsWith("counts/date=2026-10-29/hour=11/")));
    assertTrue(countKeys.stream().noneMatch(key -> key.startsWith("counts/date=2026-10-29/hour=12/")));

    List<GenericRecord> counts = S3Support.readParquet(s3, BUCKET, countKeys, tmp);
    assertEquals(
        Set.of(
            "2026-10-29T10:00:00Z SW1A 3",
            "2026-10-29T10:01:00Z SW1A 2",
            "2026-10-29T11:05:00Z EC1A 1"),
        counts.stream().map(S3SinkIntegrationTest::describe).collect(Collectors.toSet()));

    List<String> dlqKeys = S3Support.keys(s3, BUCKET, "dlq/");
    assertTrue(dlqKeys.stream().allMatch(key -> key.startsWith("dlq/date=2026-10-29/")));
    List<GenericRecord> dlqRecords = S3Support.readParquet(s3, BUCKET, dlqKeys, tmp);
    assertEquals(1, dlqRecords.size());
    assertEquals("{\"event_id\":\"late-1\"}", dlqRecords.get(0).get("payload").toString());
    assertEquals(millis("2026-10-29T12:00:00Z"), dlqRecords.get(0).get("processing_timestamp"));
  }

  private static String describe(GenericRecord record) {
    return Instant.ofEpochMilli(S3Support.timeWindowMillis(record))
        + " "
        + record.get("postcode")
        + " "
        + record.get("count");
  }

  private static long millis(String instant) {
    return Instant.parse(instant).toEpochMilli();
  }
}
