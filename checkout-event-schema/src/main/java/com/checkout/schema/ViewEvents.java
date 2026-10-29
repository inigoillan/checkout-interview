package com.checkout.schema;

import org.apache.fory.json.ForyJson;

/**
 * JSON (de)serialisation for {@link ViewEvent} backed by Apache Fory JSON
 * ({@code org.apache.fory:fory-json}), the fastest JSON codec in this stack.
 *
 * <p>Thread-safe: the underlying {@link ForyJson} instance is thread-safe and
 * shared.
 */
public final class ViewEvents {

  private static final ForyJson JSON = ForyJson.builder().build();

  private ViewEvents() {}

  public static ViewEvent fromJson(String json) {
    return JSON.fromJson(json, ViewEvent.class);
  }

  public static String toJson(ViewEvent event) {
    return JSON.toJson(event);
  }
}
