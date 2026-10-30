package com.checkout.count.sink;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;

/** Writes {@code T} as Parquet by converting each element to an Avro record. */
abstract class AvroRecordWriterFactory<T> implements BulkWriter.Factory<T> {

  private final String schemaJson;

  protected AvroRecordWriterFactory(String schemaJson) {
    this.schemaJson = schemaJson;
  }

  protected abstract GenericRecord toRecord(T element, Schema schema);

  @Override
  public BulkWriter<T> create(FSDataOutputStream out) throws IOException {
    Schema schema = new Schema.Parser().parse(schemaJson);
    BulkWriter<GenericRecord> delegate = AvroParquetWriters.forGenericRecord(schema).create(out);
    return new BulkWriter<T>() {
      @Override
      public void addElement(T element) throws IOException {
        delegate.addElement(toRecord(element, schema));
      }

      @Override
      public void flush() throws IOException {
        delegate.flush();
      }

      @Override
      public void finish() throws IOException {
        delegate.finish();
      }
    };
  }
}
