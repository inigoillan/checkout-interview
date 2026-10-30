package com.checkout.ingestor.sink;

import com.checkout.ingestor.config.IngestorConfig;
import com.checkout.ingestor.pipeline.FlinkEnvironments;
import com.checkout.ingestor.pipeline.IngestorPipeline;
import com.checkout.ingestor.source.RawEvent;
import com.checkout.ingestor.support.S3Support;
import com.checkout.ingestor.support.TestConfigs;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration-s3")
class LandingSinkTest {

  @Test
  void writes_valid_events_under_hourly_partitions_and_malformed_ones_to_the_dlq() throws Exception {
    String bucket = "lake-" + UUID.randomUUID();
    try (S3Client s3 = S3Support.client()) {
      S3Support.createBucket(s3, bucket);
      IngestorConfig config = TestConfigs.ingestor("unused:9092", "unused-topic", bucket, 1);

      StreamExecutionEnvironment env = FlinkEnvironments.create(config);
      env.setParallelism(1);
      long nine = Instant.parse("2026-10-30T09:15:00Z").getEpochSecond();
      long ten = Instant.parse("2026-10-30T10:20:00Z").getEpochSecond();
      DataStream<RawEvent> raw = env.fromData(
          new RawEvent("checkout.events.raw", 0, 0L, TestConfigs.validEvent("evt-1", nine)),
          new RawEvent("checkout.events.raw", 0, 1L, "not json"),
          new RawEvent("checkout.events.raw", 0, 2L, TestConfigs.validEvent("evt-2", ten)));
      IngestorPipeline.attach(raw, config);
      env.execute("landing-sink-test");

      List<String> landing = S3Support.parquetKeys(s3, bucket, "landing/");
      assertTrue(landing.stream().anyMatch(key -> key.startsWith("landing/date=2026-10-30/hour=09/")), landing.toString());
      assertTrue(landing.stream().anyMatch(key -> key.startsWith("landing/date=2026-10-30/hour=10/")), landing.toString());

      List<GenericRecord> landed = S3Support.readRecords(s3, bucket, landing);
      assertEquals(Set.of("evt-1", "evt-2"), landed.stream()
          .map(record -> record.get("event_id").toString())
          .collect(Collectors.toSet()));
      assertEquals(2, landed.size());

      List<String> dlq = S3Support.parquetKeys(s3, bucket, "dlq/");
      assertTrue(dlq.stream().allMatch(key -> key.matches("dlq/date=\\d{4}-\\d{2}-\\d{2}/.+\\.parquet")), dlq.toString());
      List<GenericRecord> failed = S3Support.readRecords(s3, bucket, dlq);
      assertEquals(1, failed.size());
      assertEquals("not json", failed.get(0).get("event").toString());
      assertEquals(1L, failed.get(0).get("kafka_offset"));
    }
  }
}
