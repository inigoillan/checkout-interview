package com.checkout.count.pipeline;

import com.checkout.count.model.CountRow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountWindowFunction extends ProcessWindowFunction<Integer, CountRow, String, TimeWindow> {

  @Override
  public void process(
      String postcode,
      Context context,
      Iterable<Integer> counts,
      Collector<CountRow> out) {
    out.collect(new CountRow(context.window().getStart(), postcode, counts.iterator().next()));
  }
}
