package com.checkout.count.sink;

import com.checkout.count.model.DlqRecord;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface DlqSink {

  void write(DataStream<DlqRecord> records);
}
