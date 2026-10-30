package com.checkout.qualitycheck.pipeline;

import com.checkout.qualitycheck.check.Failure;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Builds the DLQ record: {@code { "event": "<raw payload>", "failures": [{ "check": "...", "reason": "..." }] }}. */
public final class DlqEnvelope {

  private static final ObjectMapper JSON = new ObjectMapper();

  private DlqEnvelope() {}

  public static String write(Evaluation evaluation) {
    ObjectNode envelope = JSON.createObjectNode();
    envelope.put("event", evaluation.payload());
    ArrayNode failures = envelope.putArray("failures");
    for (Failure failure : evaluation.failures()) {
      ObjectNode node = failures.addObject();
      node.put("check", failure.check());
      node.put("reason", failure.reason());
    }
    return envelope.toString();
  }
}
