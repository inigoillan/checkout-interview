package com.checkout.ingestor.support;

import java.time.Duration;

public final class Waiting {

  private Waiting() {}

  @FunctionalInterface
  public interface Condition {
    boolean holds() throws Exception;
  }

  public static void until(String description, Duration timeout, Condition condition) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.holds()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out after " + timeout + " waiting for: " + description);
      }
      Thread.sleep(500);
    }
  }
}
