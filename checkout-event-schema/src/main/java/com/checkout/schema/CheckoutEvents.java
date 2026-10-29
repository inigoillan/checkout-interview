package com.checkout.schema;

import org.apache.fory.json.ForyJson;

/**
 * JSON (de)serialisation for {@link CheckoutEvent} backed by Apache Fory JSON
 * ({@code org.apache.fory:fory-json}), the fastest JSON codec in this stack.
 *
 * <p>Thread-safe: the underlying {@link ForyJson} instance is thread-safe and
 * shared.
 */
public final class CheckoutEvents {

  private static final ForyJson JSON = ForyJson.builder().build();

  private CheckoutEvents() {}

  public static CheckoutEvent fromJson(String json) {
    return JSON.fromJson(json, CheckoutEvent.class);
  }

  public static String toJson(CheckoutEvent event) {
    return JSON.toJson(event);
  }
}
