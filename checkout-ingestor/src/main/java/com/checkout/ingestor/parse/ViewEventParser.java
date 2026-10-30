package com.checkout.ingestor.parse;

import com.checkout.schema.ViewEvent;
import com.checkout.schema.ViewEvents;

import java.util.Optional;

public final class ViewEventParser {

  private ViewEventParser() {}

  public static Optional<ViewEvent> parse(String payload) {
    try {
      ViewEvent event = ViewEvents.fromJson(payload);
      if (event == null) {
        return Optional.empty();
      }
      return Optional.of(event);
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }
}
