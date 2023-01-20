package com.redis.riot.file;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

@SuppressWarnings("unchecked")
class ModulesTests extends AbstractTestcontainersRedisTestBase {

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(new RedisModulesContainer(
				RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG)));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importSugadd(RedisTestContext redis) throws Exception {
		assertExecutionSuccessful(new RiotFile().execute("import-sugadd", redis.getRedisURI(), redis.isCluster()));
		List<Suggestion<String>> suggestions = redis.sync().ftSugget("names", "Bea",
				SuggetOptions.builder().withPayloads(true).build());
		Assertions.assertEquals(5, suggestions.size());
		Assertions.assertEquals("American Blonde Ale", suggestions.get(0).getPayload());
	}

	private void assertExecutionSuccessful(int exitCode) {
		Assertions.assertEquals(0, exitCode);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void importElasticJSON(RedisTestContext redis) throws Exception {
		assertExecutionSuccessful(
				new RiotFile().execute("import-json-elastic-jsonset", redis.getRedisURI(), redis.isCluster()));
		RedisModulesCommands<String, String> sync = redis.sync();
		Assertions.assertEquals(2, sync.keys("elastic:*").size());
		ObjectMapper mapper = new ObjectMapper();
		String doc1 = sync.jsonGet("elastic:doc1");
		String expected = "{\"_index\":\"test-index\",\"_type\":\"docs\",\"_id\":\"doc1\",\"_score\":1,\"_source\":{\"name\":\"ruan\",\"age\":30,\"articles\":[\"1\",\"3\"]}}";
		Assertions.assertEquals(mapper.readTree(expected), mapper.readTree(doc1));
	}

}
