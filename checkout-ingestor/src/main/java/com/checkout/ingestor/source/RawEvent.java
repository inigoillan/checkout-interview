package com.checkout.ingestor.source;

public record RawEvent(String topic, int partition, long offset, String payload) {}
