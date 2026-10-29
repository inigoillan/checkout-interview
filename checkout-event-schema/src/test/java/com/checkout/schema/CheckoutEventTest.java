package com.checkout.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CheckoutEventTest {

  @Test
  void deserialisesSnakeCaseJson() {
    String json =
        """
        {"event_id":"evt-1","user_id":42,"postcode":"E1 6AN","webpage":"/checkout/confirm","timestamp":1718559600}
        """;

    CheckoutEvent event = CheckoutEvents.fromJson(json.trim());

    assertEquals("evt-1", event.eventId());
    assertEquals(42, event.userId());
    assertEquals("E1 6AN", event.postcode());
    assertEquals("/checkout/confirm", event.webpage());
    assertEquals(1718559600L, event.timestamp());
  }

  @Test
  void roundTrip() {
    CheckoutEvent original = new CheckoutEvent("evt-2", 7, "W1A 1AA", "/cart", 1718559700L);

    CheckoutEvent parsed = CheckoutEvents.fromJson(CheckoutEvents.toJson(original));

    assertEquals(original, parsed);
  }
}
