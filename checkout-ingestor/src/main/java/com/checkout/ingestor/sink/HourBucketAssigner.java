package com.checkout.ingestor.sink;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class HourBucketAssigner implements BucketAssigner<GenericRecord, String> {

  private static final DateTimeFormatter FORMAT =
      DateTimeFormatter.ofPattern("'date='yyyy-MM-dd'/hour='HH").withZone(ZoneOffset.UTC);

  @Override
  public String getBucketId(GenericRecord record, Context context) {
    return FORMAT.format(Instant.ofEpochSecond((Long) record.get("timestamp")));
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }
}
