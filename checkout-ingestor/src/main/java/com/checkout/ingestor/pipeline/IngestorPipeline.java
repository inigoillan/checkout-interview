package com.checkout.ingestor.pipeline;

import com.checkout.ingestor.config.IngestorConfig;
import com.checkout.ingestor.parse.FailedEvent;
import com.checkout.ingestor.parse.ParseViewEventFunction;
import com.checkout.ingestor.sink.AvroSchemas;
import com.checkout.ingestor.sink.DateBucketAssigner;
import com.checkout.ingestor.sink.FailedEventRecordMapper;
import com.checkout.ingestor.sink.HourBucketAssigner;
import com.checkout.ingestor.sink.ViewEventRecordMapper;
import com.checkout.ingestor.source.RawEvent;
import com.checkout.schema.ViewEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

public final class IngestorPipeline {

  private IngestorPipeline() {}

  public static void attach(DataStream<RawEvent> raw, IngestorConfig config) {
    SingleOutputStreamOperator<ViewEvent> parsed = raw
        .process(new ParseViewEventFunction())
        .name("parse-view-events");
    DataStream<FailedEvent> failed = parsed.getSideOutput(ParseViewEventFunction.FAILED);
    int parallelism = config.flink().sinkParallelism();

    parsed
        .map(new ProgressLogger())
        .name("progress-log")
        .map(new ViewEventRecordMapper())
        .returns(new GenericRecordAvroTypeInfo(AvroSchemas.VIEW_EVENT))
        .name("view-event-record")
        .sinkTo(landingSink(config))
        .setParallelism(parallelism)
        .name("landing-sink");

    failed
        .map(new FailedEventRecordMapper())
        .returns(new GenericRecordAvroTypeInfo(AvroSchemas.DLQ_RECORD))
        .name("dlq-record")
        .sinkTo(dlqSink(config))
        .setParallelism(parallelism)
        .name("dlq-sink");
  }

  static FileSink<GenericRecord> landingSink(IngestorConfig config) {
    return FileSink
        .forBulkFormat(
            outputPath(config, config.s3().landingPrefix()),
            AvroParquetWriters.forGenericRecord(AvroSchemas.VIEW_EVENT))
        .withBucketAssigner(new HourBucketAssigner())
        .withOutputFileConfig(parquetFiles())
        .build();
  }

  static FileSink<GenericRecord> dlqSink(IngestorConfig config) {
    return FileSink
        .forBulkFormat(
            outputPath(config, config.s3().dlqPrefix()),
            AvroParquetWriters.forGenericRecord(AvroSchemas.DLQ_RECORD))
        .withBucketAssigner(new DateBucketAssigner())
        .withOutputFileConfig(parquetFiles())
        .build();
  }

  private static Path outputPath(IngestorConfig config, String prefix) {
    return new Path("s3://" + config.s3().bucket() + "/" + prefix);
  }

  private static OutputFileConfig parquetFiles() {
    return OutputFileConfig.builder().withPartSuffix(".parquet").build();
  }
}
