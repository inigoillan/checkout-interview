package com.checkout.qualitycheck.check;

import com.checkout.qualitycheck.config.QualityCheckConfig.ChecksSettings;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class ChecksFactory {

  private ChecksFactory() {}

  public static List<ViewEventCheck> enabled(ChecksSettings settings, Clock clock) {
    List<ViewEventCheck> checks = new ArrayList<>();
    if (settings.eventId().enabled()) {
      checks.add(new EventIdCheck());
    }
    if (settings.postcode().enabled()) {
      checks.add(new PostcodeCheck());
    }
    if (settings.webpage().enabled()) {
      checks.add(new WebpageCheck());
    }
    if (settings.timestamp().enabled()) {
      checks.add(
          new TimestampCheck(
              clock,
              Duration.ofSeconds(settings.timestamp().maxAgeSeconds()),
              Duration.ofSeconds(settings.timestamp().maxFutureSeconds())));
    }
    return checks;
  }
}
