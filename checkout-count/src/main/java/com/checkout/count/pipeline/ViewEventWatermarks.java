package com.checkout.count.pipeline;

import com.checkout.count.model.TimedViewEvent;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public final class ViewEventWatermarks {

  private ViewEventWatermarks() {}

  public static WatermarkStrategy<TimedViewEvent> create(Duration bound, Duration idleness) {
    return WatermarkStrategy.<TimedViewEvent>forBoundedOutOfOrderness(bound)
        .withIdleness(idleness)
        .withTimestampAssigner((event, recordTimestamp) -> event.eventTimeMillis);
  }
}
