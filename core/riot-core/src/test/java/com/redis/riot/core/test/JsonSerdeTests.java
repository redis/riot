package com.redis.riot.core.test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.unit.DataSize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.riot.core.KeyValueDeserializer;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.util.BatchUtils;

@TestInstance(Lifecycle.PER_CLASS)
class JsonSerdeTests {

    private static final String timeseries = "{\"key\":\"gen:97\",\"type\":\"timeseries\",\"value\":[{\"timestamp\":1695939533285,\"value\":0.07027403662738285},{\"timestamp\":1695939533286,\"value\":0.7434808603018632},{\"timestamp\":1695939533287,\"value\":0.36481049906367213},{\"timestamp\":1695939533288,\"value\":0.08986928499552382},{\"timestamp\":1695939533289,\"value\":0.3901401870373925},{\"timestamp\":1695939533290,\"value\":0.1088584873055678},{\"timestamp\":1695939533291,\"value\":0.5649631025302376},{\"timestamp\":1695939533292,\"value\":0.9284983053028953},{\"timestamp\":1695939533293,\"value\":0.5009349293022067},{\"timestamp\":1695939533294,\"value\":0.7798297389022721}],\"ttl\":-1,\"memoryUsage\":0}";

    private ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    void setup() {
        mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(KeyValue.class, new KeyValueDeserializer());
        mapper.registerModule(module);
    }

    @SuppressWarnings("unchecked")
    @Test
    void deserialize() throws JsonMappingException, JsonProcessingException {
        KeyValue<String> keyValue = mapper.readValue(timeseries, KeyValue.class);
        Assertions.assertEquals("gen:97", keyValue.getKey());
    }

    @Test
    void serialize() throws JsonProcessingException {
        String key = "ts:1";
        long memoryUsage = DataSize.ofGigabytes(1).toBytes();
        long ttl = Instant.now().toEpochMilli();
        KeyValue<String> ts = new KeyValue<>();
        ts.setKey(key);
        ts.setMemoryUsage(memoryUsage);
        ts.setTtl(ttl);
        ts.setType(DataType.TIMESERIES);
        Sample sample1 = Sample.of(Instant.now().toEpochMilli(), 123.456);
        Sample sample2 = Sample.of(Instant.now().toEpochMilli() + 1000, 456.123);
        ts.setValue(Arrays.asList(sample1, sample2));
        String json = mapper.writeValueAsString(ts);
        JsonNode jsonNode = mapper.readTree(json);
        Assertions.assertEquals(key, jsonNode.get("key").asText());
        ArrayNode valueNode = (ArrayNode) jsonNode.get("value");
        Assertions.assertEquals(2, valueNode.size());
        Assertions.assertEquals(sample2.getValue(), ((DoubleNode) valueNode.get(1).get("value")).asDouble());
    }

    @SuppressWarnings("unchecked")
    @Test
    void serde() throws Exception {
        GeneratorItemReader reader = new GeneratorItemReader();
        reader.setMaxItemCount(17);
        reader.open(new ExecutionContext());
        List<KeyValue<String>> items = BatchUtils.readAll(reader);
        for (KeyValue<String> item : items) {
            String json = mapper.writeValueAsString(item);
            KeyValue<String> result = mapper.readValue(json, KeyValue.class);
            System.out.println(item.getType());
            Assertions.assertEquals(item, result);
        }
    }

}
