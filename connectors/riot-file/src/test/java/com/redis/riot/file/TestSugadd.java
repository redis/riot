package com.redis.riot.file;

import com.redis.riot.AbstractRiotTest;
import com.redislabs.mesclun.RedisModulesClient;
import com.redislabs.mesclun.api.StatefulRedisModulesConnection;
import com.redislabs.mesclun.search.Suggestion;
import com.redislabs.mesclun.search.SuggetOptions;
import com.redislabs.testcontainers.RedisModulesContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

@SuppressWarnings("unchecked")
@Testcontainers
public class TestSugadd extends AbstractRiotTest {

    @Container
    private static final RedisModulesContainer REDIS_MODULES = new RedisModulesContainer();

    @Test
    public void importSugadd() throws Exception {
        execute("import-sugadd", REDIS_MODULES);
        RedisModulesClient rediSearchClient = RedisModulesClient.create(REDIS_MODULES.getRedisURI());
        StatefulRedisModulesConnection<String, String> connection = rediSearchClient.connect();
        List<Suggestion<String>> suggestions = connection.sync().sugget("names", "Bea", SuggetOptions.builder().withPayloads(true).build());
        Assertions.assertEquals(5, suggestions.size());
        Assertions.assertEquals("American Blonde Ale", suggestions.get(0).getPayload());
    }

    @Override
    protected RiotFile app() {
        return new RiotFile();
    }
}
