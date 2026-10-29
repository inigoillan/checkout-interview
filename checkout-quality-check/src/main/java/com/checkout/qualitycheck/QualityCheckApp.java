package com.checkout.qualitycheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class QualityCheckApp {

  private static final Logger LOG = LoggerFactory.getLogger(QualityCheckApp.class);

  private QualityCheckApp() {}

  public static void main(String[] args) {
    LOG.info("Hello world from checkout-quality-check");
    System.out.println("Hello world from checkout-quality-check");
  }
}
