package com.checkout.ingestor.pipeline;

import com.checkout.ingestor.config.IngestorConfig;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public final class FlinkEnvironments {

  private FlinkEnvironments() {}

  public static StreamExecutionEnvironment create(IngestorConfig config) throws Exception {
    Configuration conf = new Configuration();
    conf.set(StateBackendOptions.STATE_BACKEND, "hashmap");
    conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpoints().location());
    conf.setString("s3.region", config.s3().region());
    if (!config.s3().accessKey().isBlank()) {
      conf.setString("s3.access-key", config.s3().accessKey());
      conf.setString("s3.secret-key", config.s3().secretKey());
    }
    if (!config.s3().endpoint().isBlank()) {
      conf.setString("s3.endpoint", config.s3().endpoint());
      conf.setString("s3.path-style-access", "true");
    }

    // The FileSink resolves the S3 filesystem while the StreamGraph is built, so the
    // S3 settings must be registered before the environment is created.
    FileSystem.initialize(conf, null);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    env.enableCheckpointing(Duration.ofSeconds(config.checkpoints().intervalSeconds()).toMillis(),
        CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().enableUnalignedCheckpoints(false);
    return env;
  }
}
