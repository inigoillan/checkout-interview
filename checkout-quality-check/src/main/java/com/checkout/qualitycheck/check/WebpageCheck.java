package com.checkout.qualitycheck.check;

import com.checkout.schema.ViewEvent;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Checks the webpage is a well formed http or https URL with a host. A value without a scheme
 * (e.g. {@code www.example.com/index.html}) is treated as https.
 */
public final class WebpageCheck implements ViewEventCheck {

  private static final Pattern HAS_SCHEME = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]*://");

  @Override
  public String name() {
    return "webpage";
  }

  @Override
  public Optional<String> violation(ViewEvent event) {
    String webpage = event.webpage();
    if (webpage == null || webpage.isEmpty()) {
      return Optional.of("webpage must not be null or empty");
    }
    String candidate = HAS_SCHEME.matcher(webpage).find() ? webpage : "https://" + webpage;
    try {
      URI uri = new URI(candidate);
      String scheme = uri.getScheme();
      if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
        return Optional.of("webpage must use the http or https scheme");
      }
      if (uri.getHost() == null) {
        return Optional.of("webpage must be a well formed URL with a host");
      }
      return Optional.empty();
    } catch (URISyntaxException e) {
      return Optional.of("webpage is not a well formed URL: " + e.getReason());
    }
  }
}
