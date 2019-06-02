package com.redhat.cajun.navy.responder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.redhat.cajun.navy.responder.message.Message;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@Configuration
@EnableKafka
public class KafkaConfiguration {

    @Autowired
    private KafkaProperties properties;

    @Bean
    public ProducerFactory<String, Message<?>> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProducer().getKeySerializer());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProducer().getValueSerializer());
        configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        configProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, properties.getSsl().getKeyStoreType());
        configProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, toAbsolutePath(properties.getSsl().getKeyStoreLocation()));
        configProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, properties.getSsl().getKeyStorePassword());
        configProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, properties.getSsl().getTrustStoreType());
        configProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, toAbsolutePath(properties.getSsl().getTrustStoreLocation()));
        configProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.getSsl().getTrustStorePassword());
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Message<?>> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getConsumer().getKeyDeserializer());
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getConsumer().getValueDeserializer());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumer().getGroupId());
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.getConsumer().getEnableAutoCommit());
        configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        configProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, properties.getSsl().getKeyStoreType());
        configProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, toAbsolutePath(properties.getSsl().getKeyStoreLocation()));
        configProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, properties.getSsl().getKeyStorePassword());
        configProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, properties.getSsl().getTrustStoreType());
        configProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, toAbsolutePath(properties.getSsl().getTrustStoreLocation()));
        configProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.getSsl().getTrustStorePassword());
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(properties.getListener().getConcurrency());
        factory.getContainerProperties().setAckMode(properties.getListener().getAckMode());
        return factory;
    }

    private String toAbsolutePath(Resource resource) {
        if (resource.isFile()) {
            try {
                return resource.getFile().getAbsolutePath();
            } catch (IOException e) {
                //ignore;
            }
        }
        return "";
    }

}
