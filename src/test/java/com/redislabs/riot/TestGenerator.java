package com.redislabs.riot;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redislabs.riot.batch.generator.GeneratorReader;

import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;

public class TestGenerator extends BaseTest {

	@Test
	public void testImportSimple() throws Exception {
		runFile("gen-simple");
		List<String> keys = commands().keys("test:*");
		Assertions.assertTrue(keys.size() > 5000);
		Map<String, String> simple123 = commands().hgetall("test:123");
		Assertions.assertTrue(simple123.containsKey(GeneratorReader.FIELD_PARTITION));
		Assertions.assertEquals("3", simple123.get(GeneratorReader.FIELD_PARTITIONS));
		Assertions.assertEquals(100, simple123.get("field1").length());
		Assertions.assertEquals(1000, simple123.get("field2").length());
	}

	@Test
	public void testImportFakerHash() throws Exception {
		runFile("gen-faker-hash");
		List<String> keys = commands().keys("person:*");
		Assertions.assertEquals(100, keys.size());
		Map<String, String> person = commands().hgetall(keys.get(0));
		Assertions.assertTrue(person.containsKey("id"));
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
	}

	@Test
	public void testImportFakerScriptProcessorHash() throws Exception {
		runFile("gen-faker-script-processor-hash");
		List<String> keys = commands().keys("person:*");
		Assertions.assertEquals(100, keys.size());
		Map<String, String> person = commands().hgetall(keys.get(0));
		Assertions.assertTrue(person.containsKey("id"));
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
		Assertions.assertEquals(person.get("address"), person.get("address").toUpperCase());
	}

	@Test
	public void testImportFakerGot() throws Exception {
		runFile("gen-faker-set");
		Set<String> names = commands().smembers("got:characters");
		Assertions.assertTrue(names.size() > 10);
		Assertions.assertTrue(names.contains("Lysa Meadows"));
	}

	@Test
	public void testImportFakerZset() throws Exception {
		runFile("gen-faker-zset");
		List<String> keys = commands().keys("leases:*");
		Assertions.assertTrue(keys.size() > 100);
		String key = keys.get(0);
		Assertions.assertTrue(commands().zcard(key) > 0);
	}

	@Test
	public void testImportStream() throws Exception {
		runFile("gen-faker-stream");
		List<StreamMessage<String, String>> messages = commands().xrange("teststream:1", Range.unbounded());
		Assertions.assertTrue(messages.size() > 0);
	}

}
