package com.checkout.count.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("unit")
class OutwardPostcodeTest {

  @ParameterizedTest
  @CsvSource({
    "SW1A 1AA, SW1A",
    "SW1A1AA, SW1A",
    "EC1A 1BB, EC1A",
    "W1A 0AX, W1A",
    "M1 1AE, M1",
    "M11AE, M1",
    "B33 8TH, B33",
    "GIR 0AA, GIR",
    "GIR0AA, GIR"
  })
  void returnsTheOutwardCodeForSpacedAndUnspacedPostcodes(String postcode, String outward) {
    assertEquals(outward, OutwardPostcode.of(postcode));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "SW1A", "SW1A 1A", "sw1a 1aa", "SW1A 1A1", "1AA SW1"})
  void rejectsValuesThatAreNotFullUkPostcodes(String postcode) {
    assertThrows(IllegalArgumentException.class, () -> OutwardPostcode.of(postcode));
  }

  @Test
  void rejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> OutwardPostcode.of(null));
  }
}
