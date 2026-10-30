package com.checkout.ingestor.parse;

import com.checkout.ingestor.support.TestConfigs;
import com.checkout.schema.ViewEvent;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ViewEventParserTest {

  @Test
  void parses_a_valid_snake_case_payload() {
    ViewEvent event = ViewEventParser.parse(TestConfigs.validEvent("evt-1", 1718559600L)).orElseThrow();

    assertEquals("evt-1", event.eventId());
    assertEquals(42, event.userId());
    assertEquals("E1 6AN", event.postcode());
    assertEquals("/checkout/confirm", event.webpage());
    assertEquals(1718559600L, event.timestamp());
  }

  @Test
  void rejects_malformed_json() {
    assertTrue(ViewEventParser.parse("{not json").isEmpty());
  }

  @Test
  void rejects_empty_payload() {
    assertTrue(ViewEventParser.parse("").isEmpty());
  }

  @Test
  void accepts_payload_missing_a_string_field_without_checking_it() {
    String payload = "{\"user_id\":42,\"postcode\":\"E1 6AN\",\"webpage\":\"/x\",\"timestamp\":1}";

    assertTrue(ViewEventParser.parse(payload).isPresent());
  }

  @Test
  void rejects_the_json_literal_null() {
    assertTrue(ViewEventParser.parse("null").isEmpty());
  }

  @Test
  void rejects_payload_with_wrong_type_for_user_id() {
    String payload = "{\"event_id\":\"evt-1\",\"user_id\":\"abc\",\"postcode\":\"E1 6AN\","
        + "\"webpage\":\"/x\",\"timestamp\":1}";

    assertTrue(ViewEventParser.parse(payload).isEmpty());
  }
}
