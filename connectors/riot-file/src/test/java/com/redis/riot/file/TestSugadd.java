package com.redis.riot.file;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.search.Suggestion;
import com.redis.lettucemod.api.search.SuggetOptions;
import com.redis.riot.AbstractRiotTests;
import com.redis.testcontainers.RedisModulesContainer;

@SuppressWarnings("unchecked")
@Testcontainers
class TestSugadd extends AbstractRiotTests {

	@Container
	private static final RedisModulesContainer REDIS = new RedisModulesContainer("preview");

	@Test
	void importSugadd() throws Exception {
		execute("import-sugadd", REDIS);
		RedisModulesClient client = RedisModulesClient.create(REDIS.getRedisURI());
		List<Suggestion<String>> suggestions = client.connect().sync().sugget("names", "Bea",
				SuggetOptions.builder().withPayloads(true).build());
		Assertions.assertEquals(5, suggestions.size());
		Assertions.assertEquals("American Blonde Ale", suggestions.get(0).getPayload());
	}

	@Override
	protected RiotFile app() {
		return new RiotFile();
	}
}
