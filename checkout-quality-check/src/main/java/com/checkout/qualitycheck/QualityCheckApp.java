package com.checkout.qualitycheck;

import com.checkout.qualitycheck.check.ChecksFactory;
import com.checkout.qualitycheck.check.QualityGate;
import com.checkout.qualitycheck.config.QualityCheckConfig;
import com.checkout.qualitycheck.config.QualityCheckConfigLoader;
import com.checkout.qualitycheck.pipeline.EventEvaluator;
import com.checkout.qualitycheck.pipeline.QualityCheckTopology;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class QualityCheckApp {

  private static final Logger LOG = LoggerFactory.getLogger(QualityCheckApp.class);

  private QualityCheckApp() {}

  public static KafkaStreams createStreams(QualityCheckConfig config, Clock clock) {
    EventEvaluator evaluator =
        new EventEvaluator(new QualityGate(ChecksFactory.enabled(config.checks(), clock)));
    Topology topology = QualityCheckTopology.build(config.kafka(), evaluator);
    return new KafkaStreams(topology, config.kafka().streamsProperties());
  }

  public static void main(String[] args) throws InterruptedException {
    QualityCheckConfig config = QualityCheckConfigLoader.load();
    KafkaStreams streams = createStreams(config, Clock.systemUTC());

    CountDownLatch shutdown = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  streams.close();
                  shutdown.countDown();
                }));

    LOG.info(
        "Starting quality check: {} -> {} (DLQ {})",
        config.kafka().inputTopic(),
        config.kafka().outputTopic(),
        config.kafka().dlqTopic());
    streams.start();
    shutdown.await();
  }
}
