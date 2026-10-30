package com.checkout.count.sink;

import com.checkout.count.config.CountConfig.Location;
import com.checkout.count.model.CountRow;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

public final class S3CountSink implements CountSink {

  private final Location location;
  private final int parallelism;

  public S3CountSink(Location location, int parallelism) {
    this.location = location;
    this.parallelism = parallelism;
  }

  @Override
  public void write(DataStream<CountRow> rows) {
    FileSink<CountRow> sink =
        FileSink.forBulkFormat(S3Paths.of(location), new CountRowWriterFactory())
            .withBucketAssigner(new TimeWindowBucketAssigner())
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build();
    rows.sinkTo(sink).setParallelism(parallelism);
  }
}
