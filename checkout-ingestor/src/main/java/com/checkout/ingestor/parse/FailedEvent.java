package com.checkout.ingestor.parse;

public record FailedEvent(String topic, int partition, long offset, String payload, long processingTimestamp) {}
