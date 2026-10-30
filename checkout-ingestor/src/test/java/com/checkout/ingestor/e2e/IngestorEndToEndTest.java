package com.checkout.ingestor.e2e;

import com.checkout.ingestor.config.IngestorConfig;
import com.checkout.ingestor.pipeline.FlinkEnvironments;
import com.checkout.ingestor.pipeline.IngestorPipeline;
import com.checkout.ingestor.source.KafkaSourceFactory;
import com.checkout.ingestor.source.RawEvent;
import com.checkout.ingestor.support.KafkaSupport;
import com.checkout.ingestor.support.S3Support;
import com.checkout.ingestor.support.TestConfigs;
import com.checkout.ingestor.support.Waiting;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("e2e")
class IngestorEndToEndTest {

  @Test
  void streams_kafka_events_into_s3_with_no_duplicates_and_malformed_events_in_the_dlq() throws Exception {
    String bootstrap = KafkaSupport.bootstrapServers();
    String topic = "raw-" + UUID.randomUUID();
    String bucket = "lake-" + UUID.randomUUID();
    KafkaSupport.createTopic(bootstrap, topic);

    try (S3Client s3 = S3Support.client()) {
      S3Support.createBucket(s3, bucket);
      IngestorConfig config = TestConfigs.ingestor(bootstrap, topic, bucket, 2);

      StreamExecutionEnvironment env = FlinkEnvironments.create(config);
      env.setParallelism(1);
      DataStream<RawEvent> raw = env.fromSource(
          KafkaSourceFactory.builder(config).build(), WatermarkStrategy.noWatermarks(), "raw");
      IngestorPipeline.attach(raw, config);
      JobClient job = env.executeAsync("ingestor-e2e");

      long now = Instant.now().getEpochSecond();
      KafkaSupport.produce(bootstrap, topic, eventsWithFailures(now));

      try {
        Waiting.until("100 landed events and 5 DLQ events", Duration.ofMinutes(3), () ->
            S3Support.readRecords(s3, bucket, S3Support.parquetKeys(s3, bucket, "landing/")).size() >= 100
                && S3Support.readRecords(s3, bucket, S3Support.parquetKeys(s3, bucket, "dlq/")).size() >= 5);
        Thread.sleep(Duration.ofSeconds(6).toMillis());

        List<GenericRecord> landed = S3Support.readRecords(s3, bucket, S3Support.parquetKeys(s3, bucket, "landing/"));
        List<GenericRecord> dlq = S3Support.readRecords(s3, bucket, S3Support.parquetKeys(s3, bucket, "dlq/"));

        assertEquals(100, landed.size());
        assertEquals(100, landedIds(landed).size());
        assertEquals(5, dlq.size());
      } finally {
        job.cancel().get();
      }
    }
  }

  private static List<String> eventsWithFailures(long timestamp) {
    List<String> payloads = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      payloads.add(TestConfigs.validEvent("evt-" + i, timestamp));
      if (i % 20 == 0) {
        payloads.add("malformed " + i);
      }
    }
    return payloads;
  }

  static Set<String> landedIds(List<GenericRecord> landed) {
    return landed.stream().map(record -> record.get("event_id").toString()).collect(Collectors.toSet());
  }
}
