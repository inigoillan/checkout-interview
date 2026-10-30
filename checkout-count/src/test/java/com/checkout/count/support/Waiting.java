package com.checkout.count.support;

import java.time.Duration;
import java.util.function.BooleanSupplier;

public final class Waiting {

  private Waiting() {}

  public static void until(String description, Duration timeout, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out after " + timeout + " waiting for " + description);
      }
      Thread.sleep(500);
    }
  }
}
