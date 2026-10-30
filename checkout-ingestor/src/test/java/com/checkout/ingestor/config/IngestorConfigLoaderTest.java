package com.checkout.ingestor.config;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class IngestorConfigLoaderTest {

  @TempDir
  Path tempDir;

  @Test
  void loads_the_bundled_defaults() throws Exception {
    IngestorConfig config = IngestorConfigLoader.load(Map.of());

    assertEquals("checkout.events.raw", config.kafka().inputTopic());
    assertEquals("checkout-lake-raw", config.s3().bucket());
    assertEquals("landing", config.s3().landingPrefix());
    assertEquals("dlq", config.s3().dlqPrefix());
    assertEquals(900L, config.checkpoints().intervalSeconds());
    assertEquals(1, config.flink().sinkParallelism());
  }

  @Test
  void environment_overrides_connection_settings() throws Exception {
    IngestorConfig config = IngestorConfigLoader.load(Map.of(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
        "AWS_ENDPOINT_URL", "http://localstack:4566",
        "AWS_REGION", "us-east-1",
        "AWS_ACCESS_KEY_ID", "key",
        "AWS_SECRET_ACCESS_KEY", "secret"));

    assertEquals("kafka:9092", config.kafka().bootstrapServers());
    assertEquals("http://localstack:4566", config.s3().endpoint());
    assertEquals("us-east-1", config.s3().region());
    assertEquals("key", config.s3().accessKey());
    assertEquals("secret", config.s3().secretKey());
  }

  @Test
  void config_file_path_overrides_the_bundled_defaults() throws Exception {
    Path file = Files.writeString(tempDir.resolve("ingestor.yaml"), """
        kafka:
          bootstrap-servers: localhost:29092
          input-topic: checkout.events.raw
          group-id: test
        s3:
          region: eu-west-1
          endpoint: ""
          bucket: test-bucket
          landing-prefix: landing
          dlq-prefix: dlq
          access-key: ""
          secret-key: ""
        checkpoints:
          location: s3://test-bucket/checkpoints
          interval-seconds: 30
        flink:
          sink-parallelism: 1
        """);

    IngestorConfig config = IngestorConfigLoader.load(Map.of("CHECKOUT_INGESTOR_CONFIG", file.toString()));

    assertEquals("test-bucket", config.s3().bucket());
    assertEquals(30L, config.checkpoints().intervalSeconds());
  }
}
