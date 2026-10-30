package com.checkout.qualitycheck.check;

import com.checkout.schema.ViewEvent;
import java.util.Optional;

public final class EventIdCheck implements ViewEventCheck {

  @Override
  public String name() {
    return "event_id";
  }

  @Override
  public Optional<String> violation(ViewEvent event) {
    String eventId = event.eventId();
    if (eventId == null || eventId.isEmpty()) {
      return Optional.of("event_id must not be null or empty");
    }
    return Optional.empty();
  }
}
