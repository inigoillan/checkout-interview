package com.checkout.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ViewEventTest {

  @Test
  void deserialisesSnakeCaseJson() {
    String json =
        """
        {"event_id":"evt-1","user_id":1234,"postcode":"SW19","webpage":"www.website.com/index.html","timestamp":1611662684}
        """;

    ViewEvent event = ViewEvents.fromJson(json.trim());

    assertEquals("evt-1", event.eventId());
    assertEquals(1234, event.userId());
    assertEquals("SW19", event.postcode());
    assertEquals("www.website.com/index.html", event.webpage());
    assertEquals(1611662684L, event.timestamp());
  }

  @Test
  void roundTrip() {
    ViewEvent original = new ViewEvent("evt-2", 4321, "SW19", "www.website.com/index.html", 1611662684L);

    ViewEvent parsed = ViewEvents.fromJson(ViewEvents.toJson(original));

    assertEquals(original, parsed);
  }
}
