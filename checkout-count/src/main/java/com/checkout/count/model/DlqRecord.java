package com.checkout.count.model;

/** A view event that arrived after its window had closed, kept for inspection. */
public class DlqRecord {

  public String payload;
  public long kafkaTimestamp;
  public long processingTimestamp;

  public DlqRecord() {}

  public DlqRecord(String payload, long kafkaTimestamp, long processingTimestamp) {
    this.payload = payload;
    this.kafkaTimestamp = kafkaTimestamp;
    this.processingTimestamp = processingTimestamp;
  }
}
