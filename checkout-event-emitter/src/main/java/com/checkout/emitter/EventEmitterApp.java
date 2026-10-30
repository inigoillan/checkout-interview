package com.checkout.emitter;

import com.checkout.emitter.config.EmitterConfig;
import com.checkout.emitter.config.EnvEmitterConfig;
import com.checkout.emitter.generator.ViewEventGenerator;
import com.checkout.schema.ViewEvents;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

import static java.lang.Thread.sleep;

public final class EventEmitterApp {

  private static final Logger LOG = LoggerFactory.getLogger(EventEmitterApp.class);

  private EventEmitterApp() {}

  public static void main(String[] args) {
    LOG.info("Starting to emit event from checkout-event-emitter");
    Integer count = parseCount(args);

    EmitterConfig config = new EnvEmitterConfig();
    String topic = parseTopic(args, config.getKafkaTopic());

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

        ViewEventGenerator generator = new ViewEventGenerator();

          int sent = 0;
          while (count == null || sent < count) {
              var viewevent = generator.next();
              String viewEventJson = ViewEvents.toJson(viewevent);
              ProducerRecord<String, String> record = new ProducerRecord<>(topic, viewevent.eventId(), viewEventJson);

              LOG.info("Sending event to checkout event-emitter topic: {}\n{}", record.topic(), viewEventJson);
              producer.send(record, ((metadata, exception) -> {
                  if (exception != null) {
                      LOG.error("Failed sending event", exception);
                  } else {
                      LOG.info("Sent with metadata topic={} partition={} offset={} timestamp={} keyBytes={} valueBytes={}",
                              metadata.topic(), metadata.partition(), metadata.offset(),
                              metadata.timestamp(), metadata.serializedKeySize(), metadata.serializedValueSize());
                  }
              }));

              sent++;
              if (count == null || sent < count) {
                try {
                  sleep(500);
                } catch (InterruptedException e) {
                  LOG.error("Interrupted while waiting", e);
                }
              }
          }
      }

  }

  static String parseTopic(String[] args, String defaultTopic) {
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals("--topic")) {
        return args[i + 1];
      }
    }
    return defaultTopic;
  }

  static Integer parseCount(String[] args) {
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals("--count")) {
        int count = Integer.parseInt(args[i + 1]);
        if (count < 1) {
          throw new IllegalArgumentException("--count must be at least 1");
        }
        return count;
      }
    }
    return null;
  }
}
