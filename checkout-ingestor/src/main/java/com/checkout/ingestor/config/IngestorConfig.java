package com.checkout.ingestor.config;

public record IngestorConfig(Kafka kafka, S3 s3, Checkpoints checkpoints, Flink flink) {

  public record Kafka(String bootstrapServers, String inputTopic, String groupId) {}

  public record S3(
      String region,
      String endpoint,
      String bucket,
      String landingPrefix,
      String dlqPrefix,
      String accessKey,
      String secretKey) {}

  public record Checkpoints(String location, long intervalSeconds) {}

  public record Flink(int sinkParallelism) {}
}
