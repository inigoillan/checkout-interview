package com.checkout.count.pipeline;

import java.util.regex.Pattern;

/**
 * Derives the outward code from a full UK postcode, spaced or unspaced: {@code SW1A 1AA} and
 * {@code SW1A1AA} both give {@code SW1A}. The inward code is always one digit and two letters,
 * so it is the last three characters once spaces are removed.
 */
public final class OutwardPostcode {

  private static final Pattern FULL_POSTCODE = Pattern.compile("^[A-Z0-9]{2,4}[0-9][A-Z]{2}$");
  private static final int INWARD_LENGTH = 3;

  private OutwardPostcode() {}

  public static String of(String postcode) {
    if (postcode == null) {
      throw new IllegalArgumentException("Cannot derive outward postcode from null: expected a full UK postcode");
    }
    String compact = postcode.replaceAll("\\s+", "");
    if (!FULL_POSTCODE.matcher(compact).matches()) {
      throw new IllegalArgumentException(
          "Cannot derive outward postcode from '" + postcode + "': expected a full UK postcode");
    }
    return compact.substring(0, compact.length() - INWARD_LENGTH);
  }
}
