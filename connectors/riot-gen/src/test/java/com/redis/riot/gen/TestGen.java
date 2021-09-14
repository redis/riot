package com.redis.riot.gen;

import com.redis.riot.RedisOptions;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.search.CreateOptions;
import com.redis.lettucemod.api.search.Document;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.search.SearchResults;
import com.redis.riot.AbstractRiotIntegrationTest;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;

import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class TestGen extends AbstractRiotIntegrationTest {

    @Container
    private static final RedisModulesContainer REDIS_MODULES = new RedisModulesContainer();

    @Override
    protected RiotGen app() {
        return new RiotGen();
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void genFakerHash(RedisServer container) throws Exception {
        execute("import-hset", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("person:*");
        Assertions.assertEquals(1000, keys.size());
        Map<String, String> person = ((RedisHashCommands<String, String>) sync).hgetall(keys.get(0));
        Assertions.assertTrue(person.containsKey("firstName"));
        Assertions.assertTrue(person.containsKey("lastName"));
        Assertions.assertTrue(person.containsKey("address"));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void genFakerSet(RedisServer container) throws Exception {
        execute("import-sadd", container);
        RedisSetCommands<String, String> sync = sync(container);
        Set<String> names = sync.smembers("got:characters");
        Assertions.assertTrue(names.size() > 10);
        for (String name : names) {
            Assertions.assertFalse(name.isEmpty());
        }
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void genFakerZset(RedisServer container) throws Exception {
        execute("import-zadd", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("leases:*");
        Assertions.assertTrue(keys.size() > 100);
        String key = keys.get(0);
        Assertions.assertTrue(((RedisSortedSetCommands<String, String>) sync).zcard(key) > 0);
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void genFakerStream(RedisServer container) throws Exception {
        execute("import-xadd", container);
        RedisStreamCommands<String, String> sync = sync(container);
        List<StreamMessage<String, String>> messages = sync.xrange("teststream:1", Range.unbounded());
        Assertions.assertTrue(messages.size() > 0);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void genFakerIndexIntrospection() throws Exception {
        String INDEX = "beerIdx";
        String FIELD_ID = "id";
        String FIELD_ABV = "abv";
        String FIELD_NAME = "name";
        String FIELD_STYLE = "style";
        String FIELD_OUNCES = "ounces";
        RedisModulesClient modulesClient = RedisModulesClient.create(REDIS_MODULES.getRedisURI());
        StatefulRedisModulesConnection<String, String> connection = modulesClient.connect();
        connection.sync().create(INDEX, CreateOptions.<String, String>builder().prefix("beer:").build(), Field.tag(FIELD_ID).sortable().build(), Field.text(FIELD_NAME).sortable().build(), Field.text(FIELD_STYLE).matcher(Field.Text.PhoneticMatcher.English).sortable().build(), Field.numeric(FIELD_ABV).sortable().build(), Field.numeric(FIELD_OUNCES).sortable().build());
        execute("import-infer", REDIS_MODULES);
        SearchResults<String, String> results = connection.sync().search(INDEX, "*");
        Assertions.assertEquals(1000, results.getCount());
        Document<String, String> doc1 = results.get(0);
        Assertions.assertNotNull(doc1.get(FIELD_ABV));
        connection.close();
        RedisOptions.shutdown(modulesClient);
    }

}
