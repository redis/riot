package com.redislabs.riot.file;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.support.XmlItemReaderBuilder;
import org.springframework.core.io.FileSystemResource;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.test.DataPopulator;

import io.lettuce.core.api.sync.RedisCommands;

public class TestXml extends AbstractFileTest {

    @Test
    public void importHmset() throws Exception {
	executeFile("/xml/import-hmset.txt");
	List<String> keys = commands().keys("trade:*");
	Assertions.assertEquals(3, keys.size());
	Map<String, String> trade1 = commands().hgetall("trade:1");
	Assertions.assertEquals("XYZ0001", trade1.get("isin"));
    }

    @SuppressWarnings({ "incomplete-switch", "rawtypes", "unchecked" })
    @Test
    public void export() throws Exception {
	DataPopulator.builder().connection(connection()).build().run();
	Path file = tempFile("redis.xml");
	executeFile("/xml/export.txt");
	XmlItemReaderBuilder<DataStructure> builder = new XmlItemReaderBuilder<>();
	builder.name("xml-file-reader");
	builder.resource(new FileSystemResource(file));
	XmlObjectReader<DataStructure> xmlObjectReader = new XmlObjectReader<>(DataStructure.class);
	xmlObjectReader.setMapper(new XmlMapper());
	builder.xmlObjectReader(xmlObjectReader);
	XmlItemReader<DataStructure<String>> reader = (XmlItemReader) builder.build();
	List<DataStructure<String>> records = readAll(reader);
	Assertions.assertEquals(commands().dbsize(), records.size());
	RedisCommands<String, String> commands = commands();
	for (DataStructure<String> record : records) {
	    String key = record.getKey();
	    switch (record.getType()) {
	    case HASH:
		Assertions.assertEquals(record.getValue(), commands.hgetall(key));
		break;
	    case STRING:
		Assertions.assertEquals(record.getValue(), commands.get(key));
		break;
	    }
	}
    }

}
