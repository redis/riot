package com.redislabs.riot.gen;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.CreateOptions;
import com.redislabs.lettusearch.index.Schema;
import com.redislabs.lettusearch.index.field.NumericField;
import com.redislabs.lettusearch.index.field.PhoneticMatcher;
import com.redislabs.lettusearch.index.field.TagField;
import com.redislabs.lettusearch.index.field.TextField;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.riot.RiotApp;
import com.redislabs.riot.test.AbstractStandaloneRedisTest;

import io.lettuce.core.Range;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;

public class TestGen extends AbstractStandaloneRedisTest {

	private RediSearchClient searchClient;

	@Override
	protected RiotApp app() {
		return new RiotGen();
	}

	@Test
	public void genFakerHash() throws Exception {
		executeFile("/import-hmset.txt");
		List<String> keys = sync.keys("person:*");
		Assertions.assertEquals(1000, keys.size());
		Map<String, String> person = sync.hgetall(keys.get(0));
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
	}

	public void genFakerScriptProcessorHash() throws Exception {
		executeFile("/script-processor.txt");
		List<String> keys = sync.keys("person:*");
		Assertions.assertEquals(100, keys.size());
		Map<String, String> person = sync.hgetall(keys.get(0));
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
		Assertions.assertEquals(person.get("address").toUpperCase(), person.get("address"));
	}

	@Test
	public void genFakerSet() throws Exception {
		executeFile("/import-sadd.txt");
		Set<String> names = sync.smembers("got:characters");
		Assertions.assertTrue(names.size() > 10);
		Assertions.assertTrue(names.contains("Lysa Meadows"));
	}

	@Test
	public void genFakerZset() throws Exception {
		executeFile("/import-zadd.txt");
		List<String> keys = sync.keys("leases:*");
		Assertions.assertTrue(keys.size() > 100);
		String key = keys.get(0);
		Assertions.assertTrue(sync.zcard(key) > 0);
	}

	@Test
	public void genFakerStream() throws Exception {
		executeFile("/import-xadd.txt");
		List<StreamMessage<String, String>> messages = sync.xrange("teststream:1", Range.unbounded());
		Assertions.assertTrue(messages.size() > 0);
	}

	@BeforeEach
	public void setupSearch() {
		searchClient = RediSearchClient.create(RedisURI.create(redis.getHost(), redis.getFirstMappedPort()));
	}

	@Test
	public void genFakerIndexIntrospection() throws Exception {
		String INDEX = "beerIntrospection";
		String FIELD_ID = "id";
		String FIELD_ABV = "abv";
		String FIELD_NAME = "name";
		String FIELD_STYLE = "style";
		String FIELD_OUNCES = "ounces";
		sync.flushall();
		Schema<String> schema = Schema.<String>builder()
				.field(TagField.<String>builder().name(FIELD_ID).sortable(true).build())
				.field(TextField.<String>builder().name(FIELD_NAME).sortable(true).build())
				.field(TextField.<String>builder().name(FIELD_STYLE).matcher(PhoneticMatcher.English).sortable(true)
						.build())
				.field(NumericField.<String>builder().name(FIELD_ABV).sortable(true).build())
				.field(NumericField.<String>builder().name(FIELD_OUNCES).sortable(true).build()).build();
		StatefulRediSearchConnection<String, String> connection = searchClient.connect();
		RediSearchCommands<String, String> searchCommands = connection.sync();
		searchCommands.create(INDEX, schema, CreateOptions.<String, String>builder().prefixes("beer:").build());
		executeFile("/index-introspection.txt");
		SearchResults<String, String> results = searchCommands.search(INDEX, "*");
		Assertions.assertEquals(1000, results.getCount());
		Document<String, String> doc1 = results.get(0);
		Assertions.assertNotNull(doc1.get(FIELD_ABV));
	}

}
