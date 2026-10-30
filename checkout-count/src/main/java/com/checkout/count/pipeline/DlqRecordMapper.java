package com.checkout.count.pipeline;

import com.checkout.count.model.DlqRecord;
import com.checkout.count.model.TimedViewEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class DlqRecordMapper implements MapFunction<TimedViewEvent, DlqRecord> {

  @Override
  public DlqRecord map(TimedViewEvent event) {
    return new DlqRecord(event.payload, event.kafkaTimestamp, System.currentTimeMillis());
  }
}
