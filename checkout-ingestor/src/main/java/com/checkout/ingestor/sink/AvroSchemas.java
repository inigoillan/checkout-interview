package com.checkout.ingestor.sink;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public final class AvroSchemas {

  public static final Schema VIEW_EVENT = load("avro/view-event.avsc");
  public static final Schema DLQ_RECORD = load("avro/dlq-record.avsc");

  private AvroSchemas() {}

  private static Schema load(String resource) {
    try (InputStream in = AvroSchemas.class.getClassLoader().getResourceAsStream(resource)) {
      if (in == null) {
        throw new IllegalStateException("Missing Avro schema " + resource);
      }
      return new Schema.Parser().parse(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
