package com.checkout.emitter.config;

public interface EmitterConfig {
    public String getKafkaBootstrapServers();
    public String getKafkaTopic();
}
