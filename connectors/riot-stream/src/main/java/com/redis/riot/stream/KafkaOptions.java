package com.redis.riot.stream;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Data;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties.Consumer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties.Producer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.ObjectUtils;
import org.springframework.util.unit.DataSize;
import picocli.CommandLine.Option;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

@Data
public class KafkaOptions {

    public enum SerDe {
        AVRO, JSON
    }

    @Option(arity = "1..*", names = "--broker", description = "One or more brokers in the form host:port.", paramLabel = "<server>")
    private String[] brokers;
    @Option(names = "--group", description = "Consumer group id.", paramLabel = "<id>")
    private String groupId = "$Default";
    @Option(names = "--registry", description = "Schema registry URL.", paramLabel = "<url>")
    private String schemaRegistryUrl;
    @Option(arity = "1..*", names = {"-p", "--property"}, description = "Additional producer/consumer properties.", paramLabel = "<k=v>")
    private Map<String, String> properties;
    @Option(names = "--serde", description = "Serializer/Deserializer: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<serde>")
    private SerDe serde = SerDe.JSON;

    public Properties consumerProperties() {
        KafkaProperties kafkaProperties = kafkaProperties();
        Consumer consumer = kafkaProperties.getConsumer();
        consumer.setGroupId(groupId);
        consumer.setKeyDeserializer(StringDeserializer.class);
        consumer.setValueDeserializer(deserializer());
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(kafkaProperties.buildConsumerProperties());
        consumerProperties.putAll(properties());
        if (serde == SerDe.JSON) {
            consumerProperties.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Map.class);
        }
        return consumerProperties;
    }

    private Map<String, Object> properties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        if (schemaRegistryUrl != null) {
            properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        }
        if (!ObjectUtils.isEmpty(this.properties)) {
            properties.putAll(this.properties);
        }
        return properties;
    }

    private KafkaProperties kafkaProperties() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        if (!ObjectUtils.isEmpty(brokers)) {
            kafkaProperties.setBootstrapServers(Arrays.asList(brokers));
        }
        return kafkaProperties;
    }

    public Map<String, Object> producerProperties() {
        KafkaProperties kafkaProperties = kafkaProperties();
        Producer producer = kafkaProperties.getProducer();
        producer.setBatchSize(DataSize.ofBytes(16384));
        producer.setBufferMemory(DataSize.ofBytes(33554432));
        producer.setKeySerializer(StringSerializer.class);
        producer.setValueSerializer(serializer());
        Map<String, Object> producerProperties = properties();
        producerProperties.putAll(kafkaProperties.buildProducerProperties());
        return producerProperties;
    }

    @SuppressWarnings("rawtypes")
    public Class<? extends Serializer> serializer() {
        switch (serde) {
            case JSON:
                return JsonSerializer.class;
            default:
                return KafkaAvroSerializer.class;
        }
    }

    @SuppressWarnings("rawtypes")
    private Class<? extends Deserializer> deserializer() {
        switch (serde) {
            case JSON:
                return JsonDeserializer.class;
            default:
                return KafkaAvroDeserializer.class;
        }
    }

}
