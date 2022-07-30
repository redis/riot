package com.redis.riot.gen;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.TextField.PhoneticMatcher;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.riot.AbstractRiotIntegrationTests;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;

@SuppressWarnings("unchecked")
class GenIntegrationTests extends AbstractRiotIntegrationTests {

	private final RedisModulesContainer redisMod = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	@Override
	protected Collection<RedisServer> redisServers() {
		Collection<RedisServer> servers = new ArrayList<>(super.redisServers());
		servers.add(redisMod);
		return servers;
	}

	@Override
	protected RiotGen app() {
		return new RiotGen();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void fakerHash(RedisTestContext redis) throws Exception {
		execute("faker-hset", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("person:*");
		Assertions.assertEquals(1000, keys.size());
		Map<String, String> person = ((RedisHashCommands<String, String>) sync).hgetall(keys.get(0));
		Assertions.assertTrue(person.containsKey("firstName"));
		Assertions.assertTrue(person.containsKey("lastName"));
		Assertions.assertTrue(person.containsKey("address"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void fakerSet(RedisTestContext redis) throws Exception {
		execute("faker-sadd", redis);
		RedisSetCommands<String, String> sync = redis.sync();
		Set<String> names = sync.smembers("got:characters");
		Assertions.assertTrue(names.size() > 10);
		for (String name : names) {
			Assertions.assertFalse(name.isEmpty());
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void fakerZset(RedisTestContext redis) throws Exception {
		execute("faker-zadd", redis);
		RedisKeyCommands<String, String> sync = redis.sync();
		List<String> keys = sync.keys("leases:*");
		Assertions.assertTrue(keys.size() > 100);
		String key = keys.get(0);
		Assertions.assertTrue(((RedisSortedSetCommands<String, String>) sync).zcard(key) > 0);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void fakerStream(RedisTestContext redis) throws Exception {
		execute("faker-xadd", redis);
		RedisStreamCommands<String, String> sync = redis.sync();
		List<StreamMessage<String, String>> messages = sync.xrange("teststream:1", Range.unbounded());
		Assertions.assertTrue(messages.size() > 0);
	}

	@Test
	void fakerInfer() throws Exception {
		Assumptions.assumeTrue(redisMod.isEnabled());
		RedisTestContext redismod = new RedisTestContext(redisMod);
		String INDEX = "beerIdx";
		String FIELD_ID = "id";
		String FIELD_ABV = "abv";
		String FIELD_NAME = "name";
		String FIELD_STYLE = "style";
		String FIELD_OUNCES = "ounces";
		redismod.sync().ftCreate(INDEX, CreateOptions.<String, String>builder().prefix("beer:").build(),
				Field.tag(FIELD_ID).sortable().build(), Field.text(FIELD_NAME).sortable().build(),
				Field.text(FIELD_STYLE).matcher(PhoneticMatcher.ENGLISH).sortable().build(),
				Field.numeric(FIELD_ABV).sortable().build(), Field.numeric(FIELD_OUNCES).sortable().build());
		execute("faker-infer", redismod);
		SearchResults<String, String> results = redismod.sync().ftSearch(INDEX, "*");
		Assertions.assertEquals(1000, results.getCount());
		Document<String, String> doc1 = results.get(0);
		Assertions.assertNotNull(doc1.get(FIELD_ABV));
		redismod.close();
	}

	@Test
	void fakerTsAdd() throws Exception {
		RedisTestContext redis = getContext(redisMod);
		execute("faker-tsadd", redis);
		List<Sample> samples = redis.sync().tsRange("ts:gen", TimeRange.unbounded(), null);
		Assertions.assertEquals(10, samples.size());
	}

	@Test
	void fakerTsAddWithOptions() throws Exception {
		RedisTestContext redis = getContext(redisMod);
		execute("faker-tsadd-options", redis);
		List<RangeResult<String, String>> results = redis.sync().tsMrange(TimeRange.unbounded(),
				MRangeOptions.<String, String>filters("character1=Einstein").build());
		Assertions.assertFalse(results.isEmpty());
	}

	@Test
	void ds() throws Exception {
		RedisTestContext redis = getContext(redisMod);
		execute("ds", redis);
		Assertions.assertEquals(1000, redis.sync().dbsize());
	}

}
