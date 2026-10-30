package com.checkout.qualitycheck.check;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.schema.ViewEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class PostcodeCheckTest {

  private final PostcodeCheck check = new PostcodeCheck();

  @ParameterizedTest
  @ValueSource(
      strings = {"SW1A1AA", "SW1A 1AA", "E16AN", "E1 6AN", "M11AE", "GIR0AA", "GIR 0AA", "W1A1AA"})
  void acceptsUppercaseFullPostcodesSpacedOrUnspaced(String postcode) {
    assertFalse(check.violation(eventWithPostcode(postcode)).isPresent());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"sw1a1aa", "sw1a 1aa", "SW19", "SW1  1AA", "SW1A1 AA", " SW1A1AA", "12345", "ZZ9"})
  void rejectsLowercaseMisplacedSpacesAndIncompletePostcodes(String postcode) {
    assertTrue(check.violation(eventWithPostcode(postcode)).isPresent());
  }

  @ParameterizedTest
  @NullAndEmptySource
  void rejectsNullOrEmpty(String postcode) {
    assertTrue(check.violation(eventWithPostcode(postcode)).isPresent());
  }

  @Test
  void reportsCheckName() {
    assertEquals("postcode", check.name());
  }

  private static ViewEvent eventWithPostcode(String postcode) {
    return new ViewEvent("evt-1", 1, postcode, "www.example.com", 0L);
  }
}
