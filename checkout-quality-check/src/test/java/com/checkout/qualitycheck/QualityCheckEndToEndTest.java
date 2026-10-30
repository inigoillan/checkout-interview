package com.checkout.qualitycheck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.checkout.qualitycheck.config.QualityCheckConfig;
import com.checkout.qualitycheck.config.QualityCheckConfig.KafkaSettings;
import com.checkout.qualitycheck.config.QualityCheckConfigLoader;
import com.checkout.schema.ViewEvent;
import com.checkout.schema.ViewEvents;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class QualityCheckEndToEndTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0"));

  @BeforeAll
  static void startKafka() {
    KAFKA.start();
  }

  @AfterAll
  static void stopKafka() {
    KAFKA.stop();
  }

  @Test
  void sanitisesValidEventsAndRoutesFailuresToDlqWithKeys() throws Exception {
    String bootstrap = KAFKA.getBootstrapServers();
    String suffix = UUID.randomUUID().toString().substring(0, 8);
    String inputTopic = "e2e.raw." + suffix;
    String outputTopic = "e2e.sanitised." + suffix;
    String dlqTopic = "e2e.dlq." + suffix;

    createTopics(bootstrap, inputTopic, outputTopic, dlqTopic);

    KafkaSettings kafka =
        new KafkaSettings(
            bootstrap,
            "quality-check-e2e-" + suffix,
            inputTopic,
            outputTopic,
            dlqTopic,
            Map.of("consumer.auto.offset.reset", "earliest"));
    QualityCheckConfig config =
        new QualityCheckConfig(kafka, QualityCheckConfigLoader.fromClasspath().checks());

    String validPayload = ViewEvents.toJson(validEvent("evt-valid"));
    String badPostcodePayload =
        ViewEvents.toJson(
            new ViewEvent(
                "evt-bad-postcode",
                42,
                "sw1a1aa",
                "www.example.com",
                Instant.now().getEpochSecond()));
    String malformedPayload = "not json";

    KafkaStreams streams = QualityCheckApp.createStreams(config, Clock.systemUTC());
    streams.start();
    try {
      produce(
          bootstrap,
          List.of(
              new ProducerRecord<>(inputTopic, "upstream-key-valid", validPayload),
              new ProducerRecord<>(inputTopic, "upstream-key-bad", badPostcodePayload),
              new ProducerRecord<>(inputTopic, null, malformedPayload)));

      List<ConsumerRecord<String, String>> sanitised = poll(bootstrap, outputTopic, 1);
      List<ConsumerRecord<String, String>> dlq = poll(bootstrap, dlqTopic, 2);

      assertEquals(1, sanitised.size());
      assertEquals("upstream-key-valid", sanitised.get(0).key());
      assertEquals(validPayload, sanitised.get(0).value());

      assertEquals(2, dlq.size());
      ConsumerRecord<String, String> badPostcode = findByKey(dlq, "evt-bad-postcode");
      JsonNode envelope = JSON.readTree(badPostcode.value());
      assertTrue(envelope.get("event").isTextual());
      assertEquals(badPostcodePayload, envelope.get("event").asText());
      assertEquals(1, envelope.get("failures").size());
      assertEquals("postcode", envelope.get("failures").get(0).get("check").asText());
      assertTrue(envelope.get("failures").get(0).get("reason").isTextual());

      ConsumerRecord<String, String> malformed =
          dlq.stream()
              .filter(record -> !"evt-bad-postcode".equals(record.key()))
              .findFirst()
              .orElseThrow();
      assertEquals(UUID.fromString(malformed.key()).toString(), malformed.key());
      assertEquals(malformedPayload, JSON.readTree(malformed.value()).get("event").asText());
    } finally {
      streams.close(Duration.ofSeconds(30));
    }
  }

  private static ConsumerRecord<String, String> findByKey(
      List<ConsumerRecord<String, String>> records, String key) {
    return records.stream().filter(record -> key.equals(record.key())).findFirst().orElseThrow();
  }

  private static ViewEvent validEvent(String eventId) {
    return new ViewEvent(eventId, 42, "SW1A1AA", "www.example.com", Instant.now().getEpochSecond());
  }

  private static void createTopics(String bootstrap, String... topics) throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
      List<NewTopic> newTopics = new ArrayList<>();
      for (String topic : topics) {
        newTopics.add(new NewTopic(topic, 1, (short) 1));
      }
      admin.createTopics(newTopics).all().get();
    }
  }

  private static void produce(String bootstrap, List<ProducerRecord<String, String>> records) {
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
      for (ProducerRecord<String, String> record : records) {
        producer.send(record);
      }
      producer.flush();
    }
  }

  private static List<ConsumerRecord<String, String>> poll(
      String bootstrap, String topic, int expected) {
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
    try (KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName()))) {
      consumer.subscribe(List.of(topic));
      while (records.size() < expected && System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
        batch.forEach(records::add);
      }
    }
    return records;
  }
}
