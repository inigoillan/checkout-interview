package com.checkout.ingestor.source;

import com.checkout.ingestor.config.IngestorConfig;
import com.checkout.ingestor.parse.FailedEvent;
import com.checkout.ingestor.parse.ParseViewEventFunction;
import com.checkout.ingestor.support.KafkaSupport;
import com.checkout.ingestor.support.TestConfigs;
import com.checkout.schema.ViewEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration-kafka")
class KafkaIngestionTest {

  @Test
  void parses_valid_events_from_kafka_and_routes_malformed_ones_to_the_failure_stream() throws Exception {
    String bootstrap = KafkaSupport.bootstrapServers();
    String topic = "raw-" + UUID.randomUUID();
    KafkaSupport.createTopic(bootstrap, topic);
    KafkaSupport.produce(bootstrap, topic, List.of(
        TestConfigs.validEvent("evt-1", 1718559600L),
        "not json",
        TestConfigs.validEvent("evt-2", 1718559601L)));

    IngestorConfig config = TestConfigs.ingestor(bootstrap, topic, "unused-bucket", 1);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<RawEvent> raw = env.fromSource(
        KafkaSourceFactory.builder(config).setBounded(OffsetsInitializer.latest()).build(),
        WatermarkStrategy.noWatermarks(),
        "raw");
    SingleOutputStreamOperator<ViewEvent> parsed = raw.process(new ParseViewEventFunction());
    DataStream<FailedEvent> failed = parsed.getSideOutput(ParseViewEventFunction.FAILED);

    List<String> results = parsed
        .map(event -> "OK " + event.eventId()).returns(String.class)
        .union(failed.map(event -> "FAILED " + event.offset()).returns(String.class))
        .executeAndCollect(3);

    assertEquals(Set.of("OK evt-1", "FAILED 1", "OK evt-2"), new HashSet<>(results));
  }
}
