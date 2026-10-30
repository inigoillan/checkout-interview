package com.checkout.count.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class CountConfigLoader {

  public static final String CONFIG_ENV = "CHECKOUT_COUNT_CONFIG";
  private static final String DEFAULT_RESOURCE = "/checkout-count.yaml";

  private static final ObjectMapper YAML =
      new ObjectMapper(new YAMLFactory())
          .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);

  private CountConfigLoader() {}

  public static CountConfig load() {
    String override = System.getenv(CONFIG_ENV);
    if (override == null || override.isBlank()) {
      return fromClasspath();
    }
    return fromFile(Path.of(override));
  }

  public static CountConfig fromClasspath() {
    try (InputStream in = CountConfigLoader.class.getResourceAsStream(DEFAULT_RESOURCE)) {
      if (in == null) {
        throw new IllegalStateException("Missing classpath resource " + DEFAULT_RESOURCE);
      }
      return YAML.readValue(in, CountConfig.class);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read " + DEFAULT_RESOURCE, e);
    }
  }

  public static CountConfig fromFile(Path path) {
    try (InputStream in = Files.newInputStream(path)) {
      return YAML.readValue(in, CountConfig.class);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read config file " + path, e);
    }
  }
}
