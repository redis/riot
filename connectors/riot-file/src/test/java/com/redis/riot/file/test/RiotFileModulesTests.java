package com.redis.riot.file.test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.riot.AbstractRiotTests;
import com.redis.riot.file.RiotFile;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

@SuppressWarnings("unchecked")
@Testcontainers
class RiotFileModulesTests extends AbstractRiotTests {

	@Container
	private static final RedisModulesContainer REDIS = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag("preview"));

	@Override
	protected Collection<RedisServer> servers() {
		return Arrays.asList(REDIS);
	}

	@Override
	protected RiotFile app() {
		return new RiotFile();
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importSugadd(RedisTestContext redis) throws Exception {
		execute("import-sugadd", redis);
		List<Suggestion<String>> suggestions = redis.sync().sugget("names", "Bea",
				SuggetOptions.builder().withPayloads(true).build());
		Assertions.assertEquals(5, suggestions.size());
		Assertions.assertEquals("American Blonde Ale", suggestions.get(0).getPayload());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importJsonElasticJsonSet(RedisTestContext redis) throws Exception {
		execute("import-json-elastic-jsonset", redis);
		RedisModulesCommands<String, String> sync = redis.sync();
		Assertions.assertEquals(2, sync.keys("elastic:*").size());
		ObjectMapper mapper = new ObjectMapper();
		String doc1 = sync.jsonGet("elastic:doc1");
		String expected = "{\"_index\":\"test-index\",\"_type\":\"docs\",\"_id\":\"doc1\",\"_score\":1,\"_source\":{\"name\":\"ruan\",\"age\":30,\"articles\":[\"1\",\"3\"]}}";
		Assertions.assertEquals(mapper.readTree(expected), mapper.readTree(doc1));
	}

}
