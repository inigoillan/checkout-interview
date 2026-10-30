package com.checkout.ingestor.parse;

import com.checkout.ingestor.source.RawEvent;
import com.checkout.schema.ViewEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public final class ParseViewEventFunction extends ProcessFunction<RawEvent, ViewEvent> {

  public static final OutputTag<FailedEvent> FAILED =
      new OutputTag<>("failed-events", TypeInformation.of(FailedEvent.class));

  private static final Logger LOG = LoggerFactory.getLogger(ParseViewEventFunction.class);

  @Override
  public void processElement(RawEvent raw, Context ctx, Collector<ViewEvent> out) {
    Optional<ViewEvent> event = ViewEventParser.parse(raw.payload());
    if (event.isPresent()) {
      out.collect(event.get());
    } else {
      LOG.warn("Unable to parse event topic={} partition={} offset={}", raw.topic(), raw.partition(), raw.offset());
      ctx.output(FAILED, new FailedEvent(
          raw.topic(), raw.partition(), raw.offset(), raw.payload(), System.currentTimeMillis()));
    }
  }
}
