package com.checkout.count.sink;

import com.checkout.count.model.DlqRecord;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

/** Places each dead-letter record under {@code date=YYYY-MM-DD} for its processing date, in UTC. */
public class DlqBucketAssigner implements BucketAssigner<DlqRecord, String> {

  private static final DateTimeFormatter PARTITION =
      DateTimeFormatter.ofPattern("'date='uuuu-MM-dd").withZone(ZoneOffset.UTC);

  @Override
  public String getBucketId(DlqRecord record, Context context) {
    return PARTITION.format(Instant.ofEpochMilli(record.processingTimestamp));
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }
}
