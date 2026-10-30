package com.checkout.qualitycheck.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.qualitycheck.check.EventIdCheck;
import com.checkout.qualitycheck.check.PostcodeCheck;
import com.checkout.qualitycheck.check.QualityGate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventEvaluatorTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private final EventEvaluator evaluator =
      new EventEvaluator(new QualityGate(List.of(new EventIdCheck(), new PostcodeCheck())));

  @Test
  void passesValidEventAndKeepsPayloadUnchanged() {
    String payload =
        "{\"event_id\":\"evt-1\",\"user_id\":42,\"postcode\":\"SW1A1AA\","
            + "\"webpage\":\"www.example.com\",\"timestamp\":1700000000}";

    Evaluation evaluation = evaluator.evaluate(payload);

    assertTrue(evaluation.passed());
    assertEquals(payload, evaluation.payload());
    assertEquals("evt-1", evaluation.eventId());
  }

  @Test
  void failsEventWithBadPostcodeAndStoresPayloadAsString() throws Exception {
    String payload =
        "{\"event_id\":\"evt-2\",\"user_id\":42,\"postcode\":\"sw1a1aa\","
            + "\"webpage\":\"www.example.com\",\"timestamp\":1700000000}";

    Evaluation evaluation = evaluator.evaluate(payload);

    assertFalse(evaluation.passed());
    assertEquals(1, evaluation.failures().size());
    assertEquals("postcode", evaluation.failures().get(0).check());
    assertEquals("evt-2", evaluation.dlqKey());

    JsonNode envelope = JSON.readTree(DlqEnvelope.write(evaluation));
    assertTrue(envelope.get("event").isTextual());
    assertEquals(payload, envelope.get("event").asText());
    assertEquals("postcode", envelope.get("failures").get(0).get("check").asText());
    assertTrue(envelope.get("failures").get(0).get("reason").isTextual());
  }

  @Test
  void missingEventIdGetsRandomUuidKey() {
    String payload =
        "{\"event_id\":\"\",\"user_id\":42,\"postcode\":\"SW1A1AA\","
            + "\"webpage\":\"www.example.com\",\"timestamp\":1700000000}";

    Evaluation evaluation = evaluator.evaluate(payload);

    assertFalse(evaluation.passed());
    assertUuid(evaluation.dlqKey());
  }

  @Test
  void failsMalformedPayloadWithPayloadCheckAndRandomUuidKey() throws Exception {
    Evaluation evaluation = evaluator.evaluate("not json");

    assertFalse(evaluation.passed());
    assertNull(evaluation.eventId());
    assertEquals("payload", evaluation.failures().get(0).check());
    assertUuid(evaluation.dlqKey());

    JsonNode envelope = JSON.readTree(DlqEnvelope.write(evaluation));
    assertEquals("not json", envelope.get("event").asText());
  }

  private static void assertUuid(String key) {
    assertEquals(key, UUID.fromString(key).toString());
  }
}
