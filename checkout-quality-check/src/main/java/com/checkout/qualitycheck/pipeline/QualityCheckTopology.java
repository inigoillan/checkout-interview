package com.checkout.qualitycheck.pipeline;

import com.checkout.qualitycheck.config.QualityCheckConfig.KafkaSettings;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public final class QualityCheckTopology {

  private QualityCheckTopology() {}

  public static Topology build(KafkaSettings kafka, EventEvaluator evaluator) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Evaluation> evaluated =
        builder
            .stream(kafka.inputTopic(), Consumed.with(Serdes.String(), Serdes.String()))
            .filter((key, payload) -> payload != null)
            .mapValues(evaluator::evaluate);

    evaluated
        .filter((key, evaluation) -> evaluation.passed())
        .mapValues(Evaluation::payload)
        .to(kafka.outputTopic(), Produced.with(Serdes.String(), Serdes.String()));

    evaluated
        .filterNot((key, evaluation) -> evaluation.passed())
        .selectKey((key, evaluation) -> evaluation.dlqKey())
        .mapValues(DlqEnvelope::write)
        .to(kafka.dlqTopic(), Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }
}
