package com.checkout.count.config;

import java.time.Duration;
import org.apache.flink.util.TimeUtils;

public record CountConfig(KafkaSettings kafka, S3Settings s3, FlinkSettings flink) {

  public Duration watermarkBound() {
    return TimeUtils.parseDuration(flink.watermark().bound());
  }

  public Duration watermarkIdleness() {
    return TimeUtils.parseDuration(flink.watermark().idleness());
  }

  public Duration checkpointInterval() {
    return TimeUtils.parseDuration(flink.checkpointing().interval());
  }

  public record KafkaSettings(String bootstrapServers, String inputTopic, String startOffset) {}

  public record S3Settings(
      String region,
      String endpoint,
      String accessKey,
      String secretKey,
      Location output,
      Location dlq,
      Checkpoints checkpoints) {}

  public record Location(String bucket, String prefix) {}

  public record Checkpoints(String location) {}

  public record FlinkSettings(
      Parallelism parallelism, Watermark watermark, Checkpointing checkpointing) {}

  public record Parallelism(int sink) {}

  public record Watermark(String bound, String idleness) {}

  public record Checkpointing(String interval, boolean unaligned) {}
}
