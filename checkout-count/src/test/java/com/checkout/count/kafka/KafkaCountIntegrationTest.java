package com.checkout.count.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.count.config.CountConfig;
import com.checkout.count.model.CountRow;
import com.checkout.count.model.TimedViewEvent;
import com.checkout.count.pipeline.CountPipeline;
import com.checkout.count.pipeline.ViewEventWatermarks;
import com.checkout.count.support.CollectingSinks;
import com.checkout.count.support.Containers;
import com.checkout.count.support.KafkaSupport;
import com.checkout.count.support.TestConfigs;
import com.checkout.count.support.Waiting;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Kafka only: no S3 is involved, so the sinks are in-memory collectors. */
@Tag("integration-kafka")
class KafkaCountIntegrationTest {

  private static final String TOPIC = "view-events-kafka-it";
  private static final String POSTCODE = "SW1A";

  @BeforeAll
  static void createTopic() throws Exception {
    KafkaSupport.createTopic(TOPIC, 2);
  }

  @Test
  void windowsFireWhenOnePartitionIsIdle() throws Exception {
    CollectingSinks.reset();
    KafkaSupport.produce(TOPIC, 0, "e1", POSTCODE, "2026-10-29T10:00:05Z");
    KafkaSupport.produce(TOPIC, 0, "e2", POSTCODE, "2026-10-29T10:00:40Z");
    KafkaSupport.produce(TOPIC, 0, "e3", POSTCODE, "2026-10-29T10:45:00Z");

    CountConfig config =
        TestConfigs.config(
            Containers.bootstrapServers(), TOPIC, "earliest", null, "unused", "1s", "2s");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    DataStream<TimedViewEvent> events =
        env.fromSource(
            KafkaSourceFactory.create(config.kafka()),
            ViewEventWatermarks.create(config.watermarkBound(), Duration.ofSeconds(2)),
            "kafka-view-events");
    new CountPipeline(CollectingSinks.countSink(), CollectingSinks.dlqSink()).attach(events);

    JobClient job = env.executeAsync("kafka-idle-partition-it");
    try {
      CountRow expected = new CountRow(millis("2026-10-29T10:00:00Z"), POSTCODE, 2);
      Waiting.until(
          "count for the 10:00 window",
          Duration.ofSeconds(90),
          () -> CollectingSinks.COUNTS.stream().anyMatch(row -> sameCount(row, expected)));
    } finally {
      job.cancel().get();
    }

    assertTrue(CollectingSinks.DLQ.isEmpty());
    assertEquals(1, CollectingSinks.COUNTS.stream().filter(row -> row.timeWindow == millis("2026-10-29T10:00:00Z")).count());
  }

  private static boolean sameCount(CountRow actual, CountRow expected) {
    return actual.timeWindow == expected.timeWindow
        && actual.postcode.equals(expected.postcode)
        && actual.count == expected.count;
  }

  private static long millis(String instant) {
    return Instant.parse(instant).toEpochMilli();
  }
}
