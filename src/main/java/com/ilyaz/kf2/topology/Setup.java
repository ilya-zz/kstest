package com.ilyaz.kf2.topology;

import com.ilyaz.kf2.handlers.UnhandlerHandler;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;

@Configuration
public class Setup {
    @Bean
    KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(new NewTopic("kf2-in", 8, (short)1),
                new NewTopic("kf2-out", 8, (short) 1));
    }

    @Bean
    KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "bathory:9092"
                ));
    }

    @Bean
    StreamsBuilderFactoryBeanConfigurer streamsCustomizer() {
        return new StreamsBuilderFactoryBeanConfigurer() {
            @Override
            public void configure(StreamsBuilderFactoryBean factoryBean) {
                factoryBean.setKafkaStreamsCustomizer(ks -> ks.setUncaughtExceptionHandler(new UnhandlerHandler()));
            }
            @Override
            public int getOrder() {
                return Integer.MAX_VALUE;
            }
        };
    }

}
