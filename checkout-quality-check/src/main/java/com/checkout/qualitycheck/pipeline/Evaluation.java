package com.checkout.qualitycheck.pipeline;

import com.checkout.qualitycheck.check.Failure;
import java.util.List;
import java.util.UUID;

/** {@code eventId} is null when the payload could not be parsed into a view event. */
public record Evaluation(String payload, String eventId, List<Failure> failures) {

  public boolean passed() {
    return failures.isEmpty();
  }

  public String dlqKey() {
    return eventId == null || eventId.isEmpty() ? UUID.randomUUID().toString() : eventId;
  }
}
