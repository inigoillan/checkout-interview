package com.checkout.count.sink;

import com.checkout.count.model.CountRow;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface CountSink {

  void write(DataStream<CountRow> rows);
}
