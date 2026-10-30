package com.checkout.qualitycheck.pipeline;

import com.checkout.qualitycheck.check.Failure;
import com.checkout.qualitycheck.check.QualityGate;
import com.checkout.schema.ViewEvent;
import com.checkout.schema.ViewEvents;
import java.util.List;

public final class EventEvaluator {

  private final QualityGate gate;

  public EventEvaluator(QualityGate gate) {
    this.gate = gate;
  }

  public Evaluation evaluate(String payload) {
    ViewEvent event;
    try {
      event = ViewEvents.fromJson(payload);
    } catch (RuntimeException e) {
      return new Evaluation(
          payload,
          null,
          List.of(new Failure("payload", "payload is not a valid view event: " + e.getMessage())));
    }
    return new Evaluation(payload, event.eventId(), gate.evaluate(event));
  }
}
