package com.checkout.qualitycheck.check;

import com.checkout.schema.ViewEvent;
import java.util.Optional;
import java.util.regex.Pattern;

/** Uppercase UK postcode, spaced (SW1A 1AA) or unspaced (SW1A1AA). */
public final class PostcodeCheck implements ViewEventCheck {

  private static final Pattern UK_POSTCODE =
      Pattern.compile(
          "^(GIR ?0AA|[A-PR-UWYZ]([0-9]{1,2}|([A-HK-Y][0-9]([0-9ABEHMNPRV-Y])?)|[0-9][A-HJKPS-UW]) ?[0-9][ABD-HJLNP-UW-Z]{2})$");

  @Override
  public String name() {
    return "postcode";
  }

  @Override
  public Optional<String> violation(ViewEvent event) {
    String postcode = event.postcode();
    if (postcode == null || !UK_POSTCODE.matcher(postcode).matches()) {
      return Optional.of("postcode must be a valid UK postcode in uppercase with no spaces");
    }
    return Optional.empty();
  }
}
