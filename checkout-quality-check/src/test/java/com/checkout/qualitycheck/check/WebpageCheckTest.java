package com.checkout.qualitycheck.check;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.schema.ViewEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class WebpageCheckTest {

  private final WebpageCheck check = new WebpageCheck();

  @ParameterizedTest
  @ValueSource(
      strings = {
        "www.website.com/index.html",
        "https://checkout.example.com/confirm",
        "http://example.com",
        "example.com:8080/path?q=1",
        "https://user:secret@example.com/path"
      })
  void acceptsWellFormedHttpAndHttpsUrls(String webpage) {
    assertFalse(check.violation(eventWithWebpage(webpage)).isPresent());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/checkout/confirm",
        "https://",
        "https:///checkout/confirm",
        "https://exa mple.com",
        "ftp://example.com/file",
        "ws://example.com"
      })
  void rejectsMalformedUrlsNonHttpSchemesAndUrlsWithoutHost(String webpage) {
    assertTrue(check.violation(eventWithWebpage(webpage)).isPresent());
  }

  @ParameterizedTest
  @NullAndEmptySource
  void rejectsNullOrEmpty(String webpage) {
    assertTrue(check.violation(eventWithWebpage(webpage)).isPresent());
  }

  @Test
  void reportsCheckName() {
    assertEquals("webpage", check.name());
  }

  private static ViewEvent eventWithWebpage(String webpage) {
    return new ViewEvent("evt-1", 1, "SW1A1AA", webpage, 0L);
  }
}
