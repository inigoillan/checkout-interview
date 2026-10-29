package com.checkout.emitter.config;

public class EnvEmitterConfig implements EmitterConfig {
    @Override
    public String getKafkaBootstrapServers() {
        return System.getenv("KAFKA_BOOTSTRAP_SERVERS");
    }

    @Override
    public String getKafkaTopic() {
//        return System.getenv("KAFKA_TOPIC");
        return "checkout.events.raw";
    }
}
