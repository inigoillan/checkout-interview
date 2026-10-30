package com.checkout.count.model;

/** A view event as read from Kafka, with its record timestamp and original JSON payload. */
public class TimedViewEvent {

  public String payload;
  public String postcode;
  public long eventTimeMillis;
  public long kafkaTimestamp;

  public TimedViewEvent() {}

  public TimedViewEvent(String payload, String postcode, long eventTimeMillis, long kafkaTimestamp) {
    this.payload = payload;
    this.postcode = postcode;
    this.eventTimeMillis = eventTimeMillis;
    this.kafkaTimestamp = kafkaTimestamp;
  }
}
