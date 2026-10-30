package com.checkout.count.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.count.CountJob;
import com.checkout.count.config.CountConfig;
import com.checkout.count.support.Containers;
import com.checkout.count.support.KafkaSupport;
import com.checkout.count.support.S3Support;
import com.checkout.count.support.TestConfigs;
import com.checkout.count.support.Waiting;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3Client;

/** Kafka and S3 together: the full job from the input topic to Parquet files. */
@Tag("e2e")
class CountEndToEndTest {

  private static final String BUCKET = "count-e2e";
  private static final String TOPIC = "checkout.events.e2e";
  private static final String HOUR_10_PREFIX = "counts/date=2026-10-29/hour=10/";

  @TempDir Path tmp;

  @Test
  void kafkaToS3WritesCountsAndRoutesLateEventsToDlq() throws Exception {
    KafkaSupport.createTopic(TOPIC, 1);
    S3Client s3 = S3Support.client();
    S3Support.createBucket(s3, BUCKET);

    CountConfig config =
        TestConfigs.config(
            Containers.bootstrapServers(), TOPIC, "earliest", Containers.s3Endpoint(), BUCKET, "2s", "2s");

    StreamExecutionEnvironment env = CountJob.createEnvironment(config);
    CountJob.define(env, config);
    JobClient job = env.executeAsync("checkout-count-e2e");
    try {
      KafkaSupport.produce(TOPIC, 0, "e1", "SW1A", "2026-10-29T10:00:10Z");
      KafkaSupport.produce(TOPIC, 0, "e2", "SW1A", "2026-10-29T10:00:50Z");
      KafkaSupport.produce(TOPIC, 0, "e3", "EC1A", "2026-10-29T10:01:10Z");
      KafkaSupport.produce(TOPIC, 0, "e4", "SW1A", "2026-10-29T11:00:00Z");

      Waiting.until(
          "hour 10 counts in S3",
          Duration.ofSeconds(120),
          () -> countRows(s3, tmp).size() >= 2);

      assertEquals(
          Set.of("2026-10-29T10:00:00Z SW1A 2", "2026-10-29T10:01:00Z EC1A 1"),
          countRows(s3, tmp));

      KafkaSupport.produce(TOPIC, 0, "late-1", "SW1A", "2026-10-29T10:00:20Z");
      String dlqPrefix = "dlq/date=" + LocalDate.now(ZoneOffset.UTC) + "/";
      Waiting.until(
          "late record in the dlq",
          Duration.ofSeconds(120),
          () -> !S3Support.keys(s3, BUCKET, dlqPrefix).isEmpty());

      List<GenericRecord> dlq =
          S3Support.readParquet(s3, BUCKET, S3Support.keys(s3, BUCKET, dlqPrefix), tmp);
      assertTrue(dlq.stream().anyMatch(record -> record.get("payload").toString().contains("late-1")));
    } finally {
      job.cancel().get();
    }
  }

  private Set<String> countRows(S3Client s3, Path dir) {
    try {
      List<String> keys = S3Support.keys(s3, BUCKET, HOUR_10_PREFIX);
      List<GenericRecord> records = S3Support.readParquet(s3, BUCKET, keys, dir);
      return records.stream()
          .map(
              record ->
                  Instant.ofEpochMilli(S3Support.timeWindowMillis(record))
                      + " "
                      + record.get("postcode")
                      + " "
                      + record.get("count"))
          .collect(Collectors.toSet());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
