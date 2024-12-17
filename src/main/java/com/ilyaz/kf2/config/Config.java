package com.ilyaz.kf2.config;

import com.ilyaz.kf2.handlers.DeserHandler;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Map;

@Configuration
public class Config {
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kcCfg() {
        return new KafkaStreamsConfiguration(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "kf2",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bathory:9092",
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,3000,
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeserHandler.class
//                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.JsonSerializer.class,
        ));
    }
}
