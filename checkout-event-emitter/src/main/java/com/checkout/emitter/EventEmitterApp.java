package com.checkout.emitter;

import com.checkout.emitter.config.EmitterConfig;
import com.checkout.emitter.config.EnvEmitterConfig;
import com.checkout.schema.ViewEvent;
import com.checkout.schema.ViewEvents;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public final class EventEmitterApp {

  private static final Logger LOG = LoggerFactory.getLogger(EventEmitterApp.class);

  private EventEmitterApp() {}

  public static void main(String[] args) {
    LOG.info("Starting to emit event from checkout-event-emitter");

    EmitterConfig config = new EnvEmitterConfig();

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

        for (int i = 0; i < 10; i++) {
          String eventId = "evt-" + i;
          var viewevent = new ViewEvent(eventId, 1234, "SW19", "https://www.checkout.com", 1234L);
          ProducerRecord<String, String> record = new ProducerRecord<>(config.getKafkaTopic(), eventId, ViewEvents.toJson(viewevent));
//          LOG.info("Sending message to checkout event-emitter topic: {}", record.topic());
          LOG.info("Sending event to checkout event-emitter topic: {}\n{}", record.topic(),  viewevent);
          producer.send(record);
        }
      }

  }
}
