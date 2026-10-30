package com.checkout.count.pipeline;

import com.checkout.count.model.TimedViewEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAggregator implements AggregateFunction<TimedViewEvent, Integer, Integer> {

  @Override
  public Integer createAccumulator() {
    return 0;
  }

  @Override
  public Integer add(TimedViewEvent event, Integer accumulator) {
    return accumulator + 1;
  }

  @Override
  public Integer getResult(Integer accumulator) {
    return accumulator;
  }

  @Override
  public Integer merge(Integer a, Integer b) {
    return a + b;
  }
}
