package com.checkout.count.support;

import com.checkout.count.config.CountConfig;
import com.checkout.count.config.CountConfig.Checkpointing;
import com.checkout.count.config.CountConfig.Checkpoints;
import com.checkout.count.config.CountConfig.FlinkSettings;
import com.checkout.count.config.CountConfig.KafkaSettings;
import com.checkout.count.config.CountConfig.Location;
import com.checkout.count.config.CountConfig.Parallelism;
import com.checkout.count.config.CountConfig.S3Settings;
import com.checkout.count.config.CountConfig.Watermark;

public final class TestConfigs {

  public static final String REGION = "us-east-1";
  public static final String ACCESS_KEY = "test";
  public static final String SECRET_KEY = "test";

  private TestConfigs() {}

  public static CountConfig config(
      String bootstrapServers,
      String inputTopic,
      String startOffset,
      String s3Endpoint,
      String bucket,
      String checkpointInterval,
      String watermarkIdleness) {
    return new CountConfig(
        new KafkaSettings(bootstrapServers, inputTopic, startOffset),
        new S3Settings(
            REGION,
            s3Endpoint,
            ACCESS_KEY,
            SECRET_KEY,
            new Location(bucket, "counts"),
            new Location(bucket, "dlq"),
            new Checkpoints("s3://" + bucket + "/checkpoints")),
        new FlinkSettings(
            new Parallelism(1),
            new Watermark("30m", watermarkIdleness),
            new Checkpointing(checkpointInterval, true)));
  }
}
