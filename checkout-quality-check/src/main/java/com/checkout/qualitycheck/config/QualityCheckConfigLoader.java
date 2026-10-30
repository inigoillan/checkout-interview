package com.checkout.qualitycheck.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class QualityCheckConfigLoader {

  public static final String CONFIG_ENV = "CHECKOUT_QUALITY_CHECK_CONFIG";
  public static final String BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";
  private static final String DEFAULT_RESOURCE = "/checkout-quality-check.yaml";

  private static final ObjectMapper YAML =
      new ObjectMapper(new YAMLFactory())
          .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);

  private QualityCheckConfigLoader() {}

  public static QualityCheckConfig load() {
    String override = System.getenv(CONFIG_ENV);
    QualityCheckConfig config =
        override == null || override.isBlank()
            ? fromClasspath()
            : fromFile(Path.of(override));
    String bootstrapServers = System.getenv(BOOTSTRAP_SERVERS_ENV);
    return bootstrapServers == null || bootstrapServers.isBlank()
        ? config
        : config.withBootstrapServers(bootstrapServers);
  }

  public static QualityCheckConfig fromClasspath() {
    try (InputStream in = QualityCheckConfigLoader.class.getResourceAsStream(DEFAULT_RESOURCE)) {
      if (in == null) {
        throw new IllegalStateException("Missing classpath resource " + DEFAULT_RESOURCE);
      }
      return YAML.readValue(in, QualityCheckConfig.class);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read " + DEFAULT_RESOURCE, e);
    }
  }

  public static QualityCheckConfig fromFile(Path path) {
    try (InputStream in = Files.newInputStream(path)) {
      return YAML.readValue(in, QualityCheckConfig.class);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read config file " + path, e);
    }
  }
}
