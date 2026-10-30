package com.checkout.ingestor;

import com.checkout.ingestor.config.IngestorConfig;
import com.checkout.ingestor.config.IngestorConfigLoader;
import com.checkout.ingestor.pipeline.FlinkEnvironments;
import com.checkout.ingestor.pipeline.IngestorPipeline;
import com.checkout.ingestor.source.KafkaSourceFactory;
import com.checkout.ingestor.source.RawEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class IngestorApp {

  private IngestorApp() {}

  public static void main(String[] args) throws Exception {
    IngestorConfig config = IngestorConfigLoader.load(System.getenv());
    StreamExecutionEnvironment env = FlinkEnvironments.create(config);

    DataStream<RawEvent> raw = env.fromSource(
        KafkaSourceFactory.builder(config).build(),
        WatermarkStrategy.noWatermarks(),
        "kafka-raw-events");

    IngestorPipeline.attach(raw, config);
    env.execute("checkout-ingestor");
  }
}
