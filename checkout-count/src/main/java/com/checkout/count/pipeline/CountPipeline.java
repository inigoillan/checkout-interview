package com.checkout.count.pipeline;

import com.checkout.count.model.CountRow;
import com.checkout.count.model.TimedViewEvent;
import com.checkout.count.sink.CountSink;
import com.checkout.count.sink.DlqSink;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

public final class CountPipeline {

  public static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

  private static final OutputTag<TimedViewEvent> LATE_EVENTS =
      new OutputTag<>("late-view-events", TypeInformation.of(TimedViewEvent.class));

  private final CountSink countSink;
  private final DlqSink dlqSink;

  public CountPipeline(CountSink countSink, DlqSink dlqSink) {
    this.countSink = countSink;
    this.dlqSink = dlqSink;
  }

  public void attach(DataStream<TimedViewEvent> events) {
    SingleOutputStreamOperator<CountRow> counts =
        events
            .keyBy(event -> OutwardPostcode.of(event.postcode), Types.STRING)
            .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
            .sideOutputLateData(LATE_EVENTS)
            .aggregate(new CountAggregator(), new CountWindowFunction());

    countSink.write(counts);
    dlqSink.write(counts.getSideOutput(LATE_EVENTS).map(new DlqRecordMapper()));
  }
}
