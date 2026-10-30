package com.checkout.qualitycheck.check;

import com.checkout.schema.ViewEvent;
import java.util.ArrayList;
import java.util.List;

public final class QualityGate {

  private final List<ViewEventCheck> checks;

  public QualityGate(List<ViewEventCheck> checks) {
    this.checks = List.copyOf(checks);
  }

  public List<Failure> evaluate(ViewEvent event) {
    List<Failure> failures = new ArrayList<>();
    for (ViewEventCheck check : checks) {
      check
          .violation(event)
          .ifPresent(reason -> failures.add(new Failure(check.name(), reason)));
    }
    return List.copyOf(failures);
  }
}
