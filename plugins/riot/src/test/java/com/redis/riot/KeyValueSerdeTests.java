package com.redis.riot;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.FileSystemResource;
import org.springframework.util.unit.DataSize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.riot.file.xml.XmlResourceItemWriter;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.ItemType;

@TestInstance(Lifecycle.PER_CLASS)
class KeyValueSerdeTests {

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
		ts.setType(ItemType.TIMESERIES.getString());
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
	void serde(TestInfo info) throws Exception {
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(17);
		reader.open(new ExecutionContext());
		KeyValue<String> item;
		while ((item = reader.read()) != null) {
			String json = mapper.writeValueAsString(item);
			KeyValue<String> result = mapper.readValue(json, KeyValue.class);
			assertEquals(item, result);
		}
		reader.close();
	}

	private <K, T> void assertEquals(KeyValue<K> source, KeyValue<K> target) {
		Assertions.assertEquals(source.getMemoryUsage(), target.getMemoryUsage());
		Assertions.assertEquals(source.getTtl(), target.getTtl());
		Assertions.assertEquals(source.getType(), target.getType());
		Assertions.assertEquals(source.getKey(), target.getKey());
		Assertions.assertEquals(source.getValue(), target.getValue());
	}

	@Test
	void test() throws Exception {
		Path directory = Files.createTempDirectory(getClass().getName());
		Path file = directory.resolve("redis.xml");
		XmlMapper mapper = new XmlMapper();
		mapper.setConfig(mapper.getSerializationConfig().withRootName("record"));
		JacksonJsonObjectMarshaller<KeyValue<String>> marshaller = new JacksonJsonObjectMarshaller<>();
		marshaller.setObjectMapper(mapper);
		XmlResourceItemWriter<KeyValue<String>> writer = new XmlResourceItemWriterBuilder<KeyValue<String>>()
				.name("xml-resource-writer").resource(new FileSystemResource(file)).rootName("root")
				.xmlObjectMarshaller(marshaller).build();
		writer.afterPropertiesSet();
		writer.open(new ExecutionContext());
		KeyValue<String> item1 = new KeyValue<>();
		item1.setKey("key1");
		item1.setTtl(123l);
		item1.setType(KeyValue.TYPE_HASH);
		Map<String, String> hash1 = Map.of("field1", "value1", "field2", "value2");
		item1.setValue(hash1);
		KeyValue<String> item2 = new KeyValue<>();
		item2.setKey("key2");
		item2.setTtl(456l);
		item2.setType(KeyValue.TYPE_STREAM);
		Map<String, String> hash2 = Map.of("field1", "value1", "field2", "value2");
		item2.setValue(hash2);
		writer.write(Chunk.of(item1, item2));
		writer.close();
		ObjectReader reader = mapper.readerFor(KeyValue.class);
		List<KeyValue<String>> keyValues = reader.<KeyValue<String>>readValues(file.toFile()).readAll();
		Assertions.assertEquals(2, keyValues.size());
		Assertions.assertEquals(item1.getKey(), keyValues.get(0).getKey());
		Assertions.assertEquals(item2.getKey(), keyValues.get(1).getKey());
		Assertions.assertEquals(item1.getTtl(), keyValues.get(0).getTtl());
		Assertions.assertEquals(item2.getTtl(), keyValues.get(1).getTtl());
		Assertions.assertEquals((Object) item1.getValue(), keyValues.get(0).getValue());
		Assertions.assertEquals((Object) item2.getValue(), keyValues.get(1).getValue());

	}

}
