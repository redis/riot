package com.redislabs.riot.gen;

import com.redislabs.lettusearch.index.Schema;
import com.redislabs.lettusearch.index.field.NumericField;
import com.redislabs.lettusearch.index.field.PhoneticMatcher;
import com.redislabs.lettusearch.index.field.TagField;
import com.redislabs.lettusearch.index.field.TextField;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.riot.test.BaseTest;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestGen extends BaseTest {

	@Override
	protected int execute(String[] args) throws Exception {
		return new RiotGen().execute(args);
	}

	@Override
	protected String applicationName() {
		return "riot-gen";
	}

	@Test
	public void genFakerHash() throws Exception {
		runFile("/import-hash.txt");
		List<String> keys = commands().keys("person:*");
		Assertions.assertEquals(100, keys.size());
		Map<String, String> person = commands().hgetall(keys.get(0));
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
	}

	public void genFakerScriptProcessorHash() throws Exception {
		runFile("/script-processor.txt");
		List<String> keys = commands().keys("person:*");
		Assertions.assertEquals(100, keys.size());
		Map<String, String> person = commands().hgetall(keys.get(0));
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
		Assertions.assertEquals(person.get("address").toUpperCase(), person.get("address"));
	}

	@Test
	public void genFakerSet() throws Exception {
		runFile("/import-set.txt");
		Set<String> names = commands().smembers("got:characters");
		Assertions.assertTrue(names.size() > 10);
		Assertions.assertTrue(names.contains("Lysa Meadows"));
	}

	@Test
	public void genFakerZset() throws Exception {
		runFile("/import-zset.txt");
		List<String> keys = commands().keys("leases:*");
		Assertions.assertTrue(keys.size() > 100);
		String key = keys.get(0);
		Assertions.assertTrue(commands().zcard(key) > 0);
	}

	@Test
	public void genFakerStream() throws Exception {
		runFile("/import-stream.txt");
		List<StreamMessage<String, String>> messages = commands().xrange("teststream:1", Range.unbounded());
		Assertions.assertTrue(messages.size() > 0);
	}

	@Test
	public void genFakerIndexIntrospection() throws Exception {
		String INDEX = "beerIntrospection";
		String FIELD_ID = "id";
		String FIELD_ABV = "abv";
		String FIELD_NAME = "name";
		String FIELD_STYLE = "style";
		String FIELD_OUNCES = "ounces";
		commands().flushall();
		Schema schema = Schema.builder().field(TagField.builder().name(FIELD_ID).sortable(true).build())
				.field(TextField.builder().name(FIELD_NAME).sortable(true).build())
				.field(TextField.builder().name(FIELD_STYLE).matcher(PhoneticMatcher.English).sortable(true).build())
				.field(NumericField.builder().name(FIELD_ABV).sortable(true).build())
				.field(NumericField.builder().name(FIELD_OUNCES).sortable(true).build()).build();
		commands().create(INDEX, schema, null);
		runFile("/index-introspection.txt");
		SearchResults<String, String> results = commands().search(INDEX, "*");
		Assertions.assertEquals(100, results.getCount());
	}

}
