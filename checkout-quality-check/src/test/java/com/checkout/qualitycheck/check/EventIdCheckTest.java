package com.checkout.qualitycheck.check;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.schema.ViewEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

class EventIdCheckTest {

  private final EventIdCheck check = new EventIdCheck();

  @Test
  void acceptsNonEmptyEventId() {
    assertFalse(check.violation(eventWithId("evt-1")).isPresent());
  }

  @ParameterizedTest
  @NullAndEmptySource
  void rejectsNullOrEmptyEventId(String eventId) {
    assertTrue(check.violation(eventWithId(eventId)).isPresent());
  }

  private static ViewEvent eventWithId(String eventId) {
    return new ViewEvent(eventId, 1, "SW1A1AA", "www.example.com", 0L);
  }
}
