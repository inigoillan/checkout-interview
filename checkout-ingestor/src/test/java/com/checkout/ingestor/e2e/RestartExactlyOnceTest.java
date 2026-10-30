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
import org.apache.flink.api.common.functions.MapFunction;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("e2e")
class RestartExactlyOnceTest {

  private static final AtomicInteger SEEN = new AtomicInteger();
  private static final AtomicBoolean INJECTED = new AtomicBoolean();

  @Test
  void a_task_failure_mid_stream_does_not_duplicate_or_lose_landed_events() throws Exception {
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
          KafkaSourceFactory.builder(config).build(), WatermarkStrategy.noWatermarks(), "raw")
          .map(new FailOnceAt(60))
          .returns(RawEvent.class);
      IngestorPipeline.attach(raw, config);
      JobClient job = env.executeAsync("ingestor-restart");

      long now = Instant.now().getEpochSecond();
      List<String> payloads = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        payloads.add(TestConfigs.validEvent("evt-" + i, now));
      }
      KafkaSupport.produce(bootstrap, topic, payloads);

      try {
        Waiting.until("100 landed events", Duration.ofMinutes(3), () ->
            S3Support.readRecords(s3, bucket, S3Support.parquetKeys(s3, bucket, "landing/")).size() >= 100);
        Thread.sleep(Duration.ofSeconds(6).toMillis());

        assertTrue(INJECTED.get(), "the failure was never injected");
        List<GenericRecord> landed = S3Support.readRecords(s3, bucket, S3Support.parquetKeys(s3, bucket, "landing/"));
        assertEquals(100, landed.size());
        assertEquals(100, IngestorEndToEndTest.landedIds(landed).size());
      } finally {
        job.cancel().get();
      }
    }
  }

  private static final class FailOnceAt implements MapFunction<RawEvent, RawEvent> {
    private final int failAt;

    FailOnceAt(int failAt) {
      this.failAt = failAt;
    }

    @Override
    public RawEvent map(RawEvent event) {
      if (SEEN.incrementAndGet() == failAt && INJECTED.compareAndSet(false, true)) {
        throw new IllegalStateException("injected task failure");
      }
      return event;
    }
  }
}
