package com.checkout.count.support;

import com.checkout.schema.ViewEvent;
import com.checkout.schema.ViewEvents;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class KafkaSupport {

  private KafkaSupport() {}

  public static void createTopic(String topic, int partitions) throws Exception {
    try (Admin admin =
        Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.bootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
    }
  }

  public static void produce(String topic, int partition, String eventId, String postcode, String timestamp)
      throws ExecutionException, InterruptedException {
    String json =
        ViewEvents.toJson(
            new ViewEvent(
                eventId,
                1234,
                postcode,
                "/checkout/confirm",
                Instant.parse(timestamp).getEpochSecond()));
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
      producer.send(new ProducerRecord<>(topic, partition, eventId, json)).get();
    }
  }

  private static Map<String, Object> producerProperties() {
    return Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.bootstrapServers(),
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  }
}
