package com.redis.riot.file;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.riot.AbstractRiotTests;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

@SuppressWarnings("unchecked")
@Testcontainers
class TestSugadd extends AbstractRiotTests {

	@Container
	private static final RedisModulesContainer REDIS = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag("preview"));

	@Override
	protected Collection<RedisServer> servers() {
		return Arrays.asList(REDIS);
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

	@Override
	protected RiotFile app() {
		return new RiotFile();
	}
}
