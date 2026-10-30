package com.checkout.qualitycheck.check;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.schema.ViewEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TimestampCheckTest {

  private static final long NOW = 1_700_000_000L;
  private static final Clock CLOCK =
      Clock.fixed(Instant.ofEpochSecond(NOW), ZoneOffset.UTC);

  private final TimestampCheck check =
      new TimestampCheck(CLOCK, Duration.ofDays(1), Duration.ofMinutes(10));

  @ParameterizedTest
  @ValueSource(longs = {NOW, NOW - 86_400L, NOW + 600L, NOW - 60L})
  void acceptsTimestampsWithinWindow(long timestamp) {
    assertFalse(check.violation(eventWithTimestamp(timestamp)).isPresent());
  }

  @ParameterizedTest
  @ValueSource(longs = {NOW - 86_401L, NOW + 601L})
  void rejectsTimestampsOutsideWindow(long timestamp) {
    assertTrue(check.violation(eventWithTimestamp(timestamp)).isPresent());
  }

  @Test
  void rejectsMillisecondTimestamps() {
    assertTrue(check.violation(eventWithTimestamp(NOW * 1000L)).isPresent());
  }

  private static ViewEvent eventWithTimestamp(long timestamp) {
    return new ViewEvent("evt-1", 1, "SW1A1AA", "www.example.com", timestamp);
  }
}
