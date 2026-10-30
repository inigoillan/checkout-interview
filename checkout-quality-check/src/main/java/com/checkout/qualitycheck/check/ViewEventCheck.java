package com.checkout.qualitycheck.check;

import com.checkout.schema.ViewEvent;
import java.util.Optional;

public interface ViewEventCheck {

  String name();

  /** Returns the reason the event fails this check, or empty if it passes. */
  Optional<String> violation(ViewEvent event);
}
