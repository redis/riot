package com.redis.riot.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.FileSystemResource;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.xml.XmlResourceItemWriter;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;

class XmlItemWriterTests {

	@Test
	void test() throws Exception {
		Path directory = Files.createTempDirectory(XmlItemWriterTests.class.getName());
		Path file = directory.resolve("redis.xml");
		XmlMapper mapper = new XmlMapper();
		mapper.setConfig(mapper.getSerializationConfig().withRootName("record"));
		JacksonJsonObjectMarshaller<KeyValue<String, Object>> marshaller = new JacksonJsonObjectMarshaller<>();
		marshaller.setObjectMapper(mapper);
		XmlResourceItemWriter<KeyValue<String, Object>> writer = new XmlResourceItemWriterBuilder<KeyValue<String, Object>>()
				.name("xml-resource-writer").resource(new FileSystemResource(file)).rootName("root")
				.xmlObjectMarshaller(marshaller).build();
		writer.afterPropertiesSet();
		writer.open(new ExecutionContext());
		KeyValue<String, Object> item1 = new KeyValue<>();
		item1.setKey("key1");
		item1.setTtl(123l);
		item1.setType(DataType.HASH.getString());
		Map<String, String> hash1 = Map.of("field1", "value1", "field2", "value2");
		item1.setValue(hash1);
		KeyValue<String, Object> item2 = new KeyValue<>();
		item2.setKey("key2");
		item2.setTtl(456l);
		item2.setType(DataType.STREAM.getString());
		Map<String, String> hash2 = Map.of("field1", "value1", "field2", "value2");
		item2.setValue(hash2);
		writer.write(Chunk.of(item1, item2));
		writer.close();
		ObjectReader reader = mapper.readerFor(KeyValue.class);
		List<KeyValue<String, Object>> keyValues = reader.<KeyValue<String, Object>>readValues(file.toFile()).readAll();
		Assertions.assertEquals(2, keyValues.size());
		Assertions.assertEquals(item1.getKey(), keyValues.get(0).getKey());
		Assertions.assertEquals(item2.getKey(), keyValues.get(1).getKey());
		Assertions.assertEquals(item1.getTtl(), keyValues.get(0).getTtl());
		Assertions.assertEquals(item2.getTtl(), keyValues.get(1).getTtl());
		Assertions.assertEquals((Object) item1.getValue(), keyValues.get(0).getValue());
		Assertions.assertEquals((Object) item2.getValue(), keyValues.get(1).getValue());

	}

}
