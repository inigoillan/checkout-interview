package com.checkout.ingestor.support;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;

public final class KafkaSupport {

  private KafkaSupport() {}

  public static String bootstrapServers() {
    return Containers.KAFKA.getBootstrapServers();
  }

  public static void createTopic(String bootstrap, String topic) throws Exception {
    try (Admin admin = Admin.create(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
    }
  }

  public static void produce(String bootstrap, String topic, List<String> payloads) {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
      for (String payload : payloads) {
        producer.send(new ProducerRecord<>(topic, payload));
      }
      producer.flush();
    }
  }
}
