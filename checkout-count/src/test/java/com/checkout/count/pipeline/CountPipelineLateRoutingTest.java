package com.checkout.count.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.count.model.CountRow;
import com.checkout.count.model.TimedViewEvent;
import com.checkout.count.support.CollectingSinks;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Drives the pipeline with a watermark that advances after every event, so the ordering of
 * window firings and late arrivals is deterministic.
 */
@Tag("unit")
class CountPipelineLateRoutingTest {

  private static final String SW1A = "SW1A";
  private static final String EC1A = "EC1A";

  @BeforeEach
  void reset() {
    CollectingSinks.reset();
  }

  @Test
  void countsWindowsOnceAndRoutesLateEventsToDlq() throws Exception {
    List<TimedViewEvent> events =
        List.of(
            event("on-time-1", SW1A, "2026-10-29T10:00:10Z"),
            event("on-time-2", SW1A, "2026-10-29T10:01:05Z"),
            event("advances-watermark", SW1A, "2026-10-29T10:31:00Z"),
            event("late", SW1A, "2026-10-29T10:00:40Z"),
            event("advances-further", EC1A, "2026-10-29T11:00:00Z"));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<TimedViewEvent> stream =
        env.fromData(events, TypeInformation.of(TimedViewEvent.class))
            .assignTimestampsAndWatermarks(perEventWatermarks());

    new CountPipeline(CollectingSinks.countSink(), CollectingSinks.dlqSink()).attach(stream);
    env.execute("late-routing-test");

    List<CountRow> counts = new ArrayList<>(CollectingSinks.COUNTS);
    counts.sort(Comparator.comparingLong(row -> row.timeWindow));
    assertEquals(
        List.of(
            new CountRow(millis("2026-10-29T10:00:00Z"), SW1A, 1),
            new CountRow(millis("2026-10-29T10:01:00Z"), SW1A, 1),
            new CountRow(millis("2026-10-29T10:31:00Z"), SW1A, 1),
            new CountRow(millis("2026-10-29T11:00:00Z"), EC1A, 1)),
        counts);

    assertEquals(1, CollectingSinks.DLQ.size());
    assertEquals("late", CollectingSinks.DLQ.get(0).payload);
    assertTrue(CollectingSinks.DLQ.get(0).processingTimestamp > 0);
  }

  private static WatermarkStrategy<TimedViewEvent> perEventWatermarks() {
    return WatermarkStrategy.<TimedViewEvent>forGenerator(new PerEventWatermarks())
        .withTimestampAssigner((event, recordTimestamp) -> event.eventTimeMillis);
  }

  private static TimedViewEvent event(String payload, String postcode, String timestamp) {
    return new TimedViewEvent(payload, postcode, millis(timestamp), 0L);
  }

  private static long millis(String instant) {
    return Instant.parse(instant).toEpochMilli();
  }

  private static final class PerEventWatermarks implements WatermarkGeneratorSupplier<TimedViewEvent> {

    private static final long BOUND_MILLIS = Duration.ofMinutes(30).toMillis();

    @Override
    public WatermarkGenerator<TimedViewEvent> createWatermarkGenerator(Context context) {
      return new WatermarkGenerator<>() {
        private long maxTimestamp = Long.MIN_VALUE + BOUND_MILLIS;

        @Override
        public void onEvent(TimedViewEvent event, long eventTimestamp, WatermarkOutput output) {
          maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
          output.emitWatermark(new Watermark(maxTimestamp - BOUND_MILLIS));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
      };
    }
  }
}
