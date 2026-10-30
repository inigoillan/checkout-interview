package com.checkout.count.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("unit")
class CountConfigLoaderTest {

  @TempDir Path dir;

  @Test
  void loadsDefaultsFromClasspath() {
    CountConfig config = CountConfigLoader.fromClasspath();

    assertEquals("checkout.events.sanitised", config.kafka().inputTopic());
    assertEquals("earliest", config.kafka().startOffset());
    assertEquals("checkout-lake-raw", config.s3().output().bucket());
    assertEquals("counts", config.s3().output().prefix());
    assertEquals("dlq", config.s3().dlq().prefix());
    assertEquals("s3://checkout-checkpoints/checkout-count", config.s3().checkpoints().location());
    assertEquals(1, config.flink().parallelism().sink());
    assertEquals(Duration.ofMinutes(30), config.watermarkBound());
    assertEquals(Duration.ofMinutes(1), config.watermarkIdleness());
    assertEquals(Duration.ofMinutes(5), config.checkpointInterval());
    assertTrue(config.flink().checkpointing().unaligned());
  }

  @Test
  void loadsOverridesFromFile() throws Exception {
    Path file = dir.resolve("override.yaml");
    Files.writeString(
        file,
        """
        kafka:
          bootstrap-servers: kafka:9092
          input-topic: other.topic
          start-offset: latest
        s3:
          region: eu-west-2
          output:
            bucket: my-bucket
            prefix: out
          dlq:
            bucket: my-bucket
            prefix: late
          checkpoints:
            location: s3://my-bucket/cp
        flink:
          parallelism:
            sink: 2
          watermark:
            bound: 10m
            idleness: 30s
          checkpointing:
            interval: 1m
            unaligned: false
        """);

    CountConfig config = CountConfigLoader.fromFile(file);

    assertEquals("kafka:9092", config.kafka().bootstrapServers());
    assertEquals("other.topic", config.kafka().inputTopic());
    assertEquals("latest", config.kafka().startOffset());
    assertEquals("eu-west-2", config.s3().region());
    assertEquals("late", config.s3().dlq().prefix());
    assertEquals(2, config.flink().parallelism().sink());
    assertEquals(Duration.ofMinutes(10), config.watermarkBound());
    assertEquals(Duration.ofSeconds(30), config.watermarkIdleness());
    assertEquals(Duration.ofMinutes(1), config.checkpointInterval());
    assertFalse(config.flink().checkpointing().unaligned());
  }
}
