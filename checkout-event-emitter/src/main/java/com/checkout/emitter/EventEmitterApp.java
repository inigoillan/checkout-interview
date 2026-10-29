package com.checkout.emitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventEmitterApp {

  private static final Logger LOG = LoggerFactory.getLogger(EventEmitterApp.class);

  private EventEmitterApp() {}

  public static void main(String[] args) {
    LOG.info("Hello world from checkout-event-emitter");
    System.out.println("Hello world from checkout-event-emitter");
  }
}
