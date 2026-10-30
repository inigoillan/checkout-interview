package com.checkout.ingestor.support;

import com.checkout.ingestor.config.IngestorConfig;

import java.util.List;

public final class TestConfigs {

  private TestConfigs() {}

  public static IngestorConfig ingestor(String kafkaBootstrap, String topic, String bucket, long intervalSeconds) {
    return new IngestorConfig(
        new IngestorConfig.Kafka(kafkaBootstrap, topic, "checkout-ingestor-test"),
        new IngestorConfig.S3(
            Containers.LOCALSTACK.getRegion(),
            Containers.LOCALSTACK.getEndpoint().toString(),
            bucket,
            "landing",
            "dlq",
            Containers.LOCALSTACK.getAccessKey(),
            Containers.LOCALSTACK.getSecretKey()),
        new IngestorConfig.Checkpoints("s3://" + bucket + "/checkpoints", intervalSeconds),
        new IngestorConfig.Flink(1));
  }

  public static String validEvent(String eventId, long timestamp) {
    return "{\"event_id\":\"" + eventId + "\",\"user_id\":42,\"postcode\":\"E1 6AN\","
        + "\"webpage\":\"/checkout/confirm\",\"timestamp\":" + timestamp + "}";
  }

  public static List<String> validEvents(int count, long timestamp, String prefix) {
    return java.util.stream.IntStream.range(0, count)
        .mapToObj(i -> validEvent(prefix + i, timestamp))
        .toList();
  }
}
