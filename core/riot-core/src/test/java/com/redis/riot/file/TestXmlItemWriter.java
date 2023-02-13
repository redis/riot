package com.redis.riot.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.FileSystemResource;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.resource.XmlResourceItemWriter;
import com.redis.riot.file.resource.XmlResourceItemWriterBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;

class TestXmlItemWriter {

	@Test
	void test() throws Exception {
		Path directory = Files.createTempDirectory(TestXmlItemWriter.class.getName());
		Path file = directory.resolve("redis.xml");
		XmlMapper mapper = new XmlMapper();
		mapper.setConfig(mapper.getSerializationConfig().withRootName("record"));
		JacksonJsonObjectMarshaller<DataStructure<String>> marshaller = new JacksonJsonObjectMarshaller<>();
		marshaller.setObjectMapper(mapper);
		XmlResourceItemWriter<DataStructure<String>> writer = new XmlResourceItemWriterBuilder<DataStructure<String>>()
				.name("xml-resource-writer").resource(new FileSystemResource(file)).rootName("root")
				.xmlObjectMarshaller(marshaller).build();
		writer.afterPropertiesSet();
		writer.open(new ExecutionContext());
		DataStructure<String> item1 = new DataStructure<>();
		item1.setKey("key1");
		item1.setTtl(123l);
		item1.setType(Type.HASH);
		Map<String, String> hash1 = Map.of("field1", "value1", "field2", "value2");
		item1.setValue(hash1);
		DataStructure<String> item2 = new DataStructure<>();
		item2.setKey("key2");
		item2.setTtl(456l);
		item2.setType(Type.STREAM);
		Map<String, String> hash2 = Map.of("field1", "value1", "field2", "value2");
		item2.setValue(hash2);
		writer.write(Arrays.asList(item1, item2));
		writer.close();
		ObjectReader reader = mapper.readerFor(DataStructure.class);
		List<DataStructure<String>> keyValues = reader.<DataStructure<String>>readValues(file.toFile()).readAll();
		Assertions.assertEquals(2, keyValues.size());
		Assertions.assertEquals(item1.getKey(), keyValues.get(0).getKey());
		Assertions.assertEquals(item2.getKey(), keyValues.get(1).getKey());
		Assertions.assertEquals(item1.getTtl(), keyValues.get(0).getTtl());
		Assertions.assertEquals(item2.getTtl(), keyValues.get(1).getTtl());
		Assertions.assertEquals(item1.getValue(), keyValues.get(0).getValue());
		Assertions.assertEquals(item2.getValue(), keyValues.get(1).getValue());

	}

}
