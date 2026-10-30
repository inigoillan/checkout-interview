package com.checkout.ingestor.pipeline;

import com.checkout.schema.ViewEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProgressLogger implements MapFunction<ViewEvent, ViewEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ProgressLogger.class);
  private static final long LOG_EVERY = 10;

  private long processed;

  @Override
  public ViewEvent map(ViewEvent event) {
    processed++;
    if (processed % LOG_EVERY == 0) {
      LOG.info("Processed {} events in this task", processed);
    }
    return event;
  }
}
