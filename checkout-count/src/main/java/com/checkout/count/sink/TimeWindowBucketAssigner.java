package com.checkout.count.sink;

import com.checkout.count.model.CountRow;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

/** Places each count row under {@code date=YYYY-MM-DD/hour=HH} for its window start, in UTC. */
public class TimeWindowBucketAssigner implements BucketAssigner<CountRow, String> {

  private static final DateTimeFormatter PARTITION =
      DateTimeFormatter.ofPattern("'date='uuuu-MM-dd'/hour='HH").withZone(ZoneOffset.UTC);

  @Override
  public String getBucketId(CountRow row, Context context) {
    return PARTITION.format(Instant.ofEpochMilli(row.timeWindow));
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }
}
