package com.checkout.count.support;

import com.checkout.count.model.CountRow;
import com.checkout.count.model.DlqRecord;
import com.checkout.count.sink.CountSink;
import com.checkout.count.sink.DlqSink;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink;

/** In-memory sinks for tests that must run without S3. Records are kept in static lists per JVM. */
public final class CollectingSinks {

  public static final List<CountRow> COUNTS = Collections.synchronizedList(new ArrayList<>());
  public static final List<DlqRecord> DLQ = Collections.synchronizedList(new ArrayList<>());

  private CollectingSinks() {}

  public static void reset() {
    COUNTS.clear();
    DLQ.clear();
  }

  public static CountSink countSink() {
    return CollectingSinks::collectCounts;
  }

  public static DlqSink dlqSink() {
    return CollectingSinks::collectDlq;
  }

  private static void collectCounts(DataStream<CountRow> rows) {
    rows.map(new CollectCounts()).addSink(new DiscardingSink<>());
  }

  private static void collectDlq(DataStream<DlqRecord> records) {
    records.map(new CollectDlq()).addSink(new DiscardingSink<>());
  }

  private static final class CollectCounts implements MapFunction<CountRow, CountRow> {
    @Override
    public CountRow map(CountRow row) {
      COUNTS.add(row);
      return row;
    }
  }

  private static final class CollectDlq implements MapFunction<DlqRecord, DlqRecord> {
    @Override
    public DlqRecord map(DlqRecord record) {
      DLQ.add(record);
      return record;
    }
  }
}
