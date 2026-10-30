package com.checkout.ingestor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class IngestorConfigLoader {

  public static final String CONFIG_PATH_ENV = "CHECKOUT_INGESTOR_CONFIG";

  private static final String DEFAULT_RESOURCE = "checkout-ingestor.yaml";

  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
      .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);

  private IngestorConfigLoader() {}

  public static IngestorConfig load(Map<String, String> env) throws IOException {
    IngestorConfig fromFile = read(env);
    return applyEnvironment(fromFile, env);
  }

  private static IngestorConfig read(Map<String, String> env) throws IOException {
    String path = env.get(CONFIG_PATH_ENV);
    try (InputStream in = path != null ? Files.newInputStream(Path.of(path)) : defaultResource()) {
      return YAML.readValue(in, IngestorConfig.class);
    }
  }

  private static InputStream defaultResource() throws FileNotFoundException {
    InputStream in = IngestorConfigLoader.class.getClassLoader().getResourceAsStream(DEFAULT_RESOURCE);
    if (in == null) {
      throw new FileNotFoundException("Missing classpath resource " + DEFAULT_RESOURCE);
    }
    return in;
  }

  static IngestorConfig applyEnvironment(IngestorConfig config, Map<String, String> env) {
    IngestorConfig.Kafka kafka = config.kafka();
    IngestorConfig.S3 s3 = config.s3();
    return new IngestorConfig(
        new IngestorConfig.Kafka(
            env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", kafka.bootstrapServers()),
            kafka.inputTopic(),
            kafka.groupId()),
        new IngestorConfig.S3(
            env.getOrDefault("AWS_REGION", s3.region()),
            env.getOrDefault("AWS_ENDPOINT_URL", s3.endpoint()),
            s3.bucket(),
            s3.landingPrefix(),
            s3.dlqPrefix(),
            env.getOrDefault("AWS_ACCESS_KEY_ID", s3.accessKey()),
            env.getOrDefault("AWS_SECRET_ACCESS_KEY", s3.secretKey())),
        config.checkpoints(),
        config.flink());
  }
}
