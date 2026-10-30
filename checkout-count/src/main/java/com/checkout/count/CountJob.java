package com.checkout.count;

import com.checkout.count.config.CountConfig;
import com.checkout.count.config.CountConfigLoader;
import com.checkout.count.kafka.KafkaSourceFactory;
import com.checkout.count.model.TimedViewEvent;
import com.checkout.count.pipeline.CountPipeline;
import com.checkout.count.pipeline.ViewEventWatermarks;
import com.checkout.count.sink.S3CountSink;
import com.checkout.count.sink.S3DlqSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CountJob {

  private static final Logger LOG = LoggerFactory.getLogger(CountJob.class);

  private CountJob() {}

  public static void main(String[] args) throws Exception {
    CountConfig config = CountConfigLoader.load();
    StreamExecutionEnvironment env = createEnvironment(config);
    define(env, config);
    env.execute("checkout-count");
  }

  public static StreamExecutionEnvironment createEnvironment(CountConfig config) {
    CountConfig.S3Settings s3 = config.s3();

    Configuration conf = new Configuration();
    conf.setString("state.backend.type", "rocksdb");
    conf.setString("state.checkpoints.dir", s3.checkpoints().location());
    conf.setString("s3.region", s3.region());
    setIfPresent(conf, "s3.endpoint", firstNonBlank(s3.endpoint(), System.getenv("AWS_ENDPOINT_URL")));
    setIfPresent(conf, "s3.access-key", firstNonBlank(s3.accessKey(), System.getenv("AWS_ACCESS_KEY_ID")));
    setIfPresent(conf, "s3.secret-key", firstNonBlank(s3.secretKey(), System.getenv("AWS_SECRET_ACCESS_KEY")));
    conf.setString("s3.path-style-access", "true");
    FileSystem.initialize(conf);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    env.enableCheckpointing(config.checkpointInterval().toMillis(), CheckpointingMode.EXACTLY_ONCE);
    CheckpointConfig checkpoints = env.getCheckpointConfig();
    checkpoints.enableUnalignedCheckpoints(config.flink().checkpointing().unaligned());
    return env;
  }

  public static void define(StreamExecutionEnvironment env, CountConfig config) {
    DataStream<TimedViewEvent> events =
        env.fromSource(
            KafkaSourceFactory.create(config.kafka()),
            ViewEventWatermarks.create(config.watermarkBound(), config.watermarkIdleness()),
            "kafka-view-events");

    int sinkParallelism = config.flink().parallelism().sink();
    new CountPipeline(
            new S3CountSink(config.s3().output(), sinkParallelism),
            new S3DlqSink(config.s3().dlq(), sinkParallelism))
        .attach(events);
    LOG.info("checkout-count pipeline defined for topic {}", config.kafka().inputTopic());
  }

  private static void setIfPresent(Configuration conf, String key, String value) {
    if (value != null && !value.isBlank()) {
      conf.setString(key, value);
    }
  }

  private static String firstNonBlank(String configured, String fromEnv) {
    return configured != null && !configured.isBlank() ? configured : fromEnv;
  }
}
