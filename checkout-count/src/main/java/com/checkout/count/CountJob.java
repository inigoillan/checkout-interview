package com.checkout.count;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CountJob {

  private static final Logger LOG = LoggerFactory.getLogger(CountJob.class);

  private CountJob() {}

  public static void main(String[] args) {
    LOG.info("Hello world from checkout-count");
    System.out.println("Hello world from checkout-count");
  }
}
