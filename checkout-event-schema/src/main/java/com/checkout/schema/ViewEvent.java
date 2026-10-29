package com.checkout.schema;

import org.apache.fory.json.annotation.JsonProperty;

/**
 * Canonical view event shared by all modules.
 *
 * <p>Wire format is JSON with snake_case field names:
 * <pre>
 * {
 *   "event_id": "evt-1",
 *   "user_id": 42,
 *   "postcode": "E1 6AN",
 *   "webpage": "/checkout/confirm",
 *   "timestamp": 1718559600
 * }
 * </pre>
 * {@code timestamp} is expressed in seconds since the epoch.
 */
public record ViewEvent(
    @JsonProperty("event_id") String eventId,
    @JsonProperty("user_id") int userId,
    @JsonProperty("postcode") String postcode,
    @JsonProperty("webpage") String webpage,
    @JsonProperty("timestamp") long timestamp) {}
