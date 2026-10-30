package com.checkout.qualitycheck.check;

import com.checkout.schema.ViewEvent;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

/** Timestamp is in epoch seconds and within [now - maxAge, now + maxFuture]. */
public final class TimestampCheck implements ViewEventCheck {

  private final Clock clock;
  private final Duration maxAge;
  private final Duration maxFuture;

  public TimestampCheck(Clock clock, Duration maxAge, Duration maxFuture) {
    this.clock = clock;
    this.maxAge = maxAge;
    this.maxFuture = maxFuture;
  }

  @Override
  public String name() {
    return "timestamp";
  }

  @Override
  public Optional<String> violation(ViewEvent event) {
    long now = clock.instant().getEpochSecond();
    long earliest = now - maxAge.getSeconds();
    long latest = now + maxFuture.getSeconds();
    long timestamp = event.timestamp();
    if (timestamp < earliest || timestamp > latest) {
      return Optional.of(
          "timestamp "
              + timestamp
              + " must be in epoch seconds and between "
              + earliest
              + " and "
              + latest);
    }
    return Optional.empty();
  }
}
