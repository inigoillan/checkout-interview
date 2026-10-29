package com.checkout.ingestor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IngestorApp {

  private static final Logger LOG = LoggerFactory.getLogger(IngestorApp.class);

  private IngestorApp() {}

  public static void main(String[] args) {
    LOG.info("Hello world from checkout-ingestor");
    System.out.println("Hello world from checkout-ingestor");
  }
}
