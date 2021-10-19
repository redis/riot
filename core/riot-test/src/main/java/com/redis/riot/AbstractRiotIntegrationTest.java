package com.redis.riot;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.DataGenerator;
import com.redis.spring.batch.DataGenerator.DataGeneratorBuilder;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;

@SuppressWarnings("unchecked")
@Testcontainers
public abstract class AbstractRiotIntegrationTest extends AbstractRiotTest {

    @Container
    private static final RedisContainer REDIS = new RedisContainer().withKeyspaceNotifications();
    @Container
    private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer().withKeyspaceNotifications();

    protected static final Map<RedisServer, AbstractRedisClient> CLIENTS = new HashMap<>();
    protected static final Map<RedisServer, GenericObjectPool<? extends StatefulConnection<String, String>>> POOLS = new HashMap<>();
    protected static final Map<RedisServer, StatefulConnection<String, String>> CONNECTIONS = new HashMap<>();
    protected static final Map<RedisServer, StatefulRedisPubSubConnection<String, String>> PUBSUB_CONNECTIONS = new HashMap<>();
    protected static final Map<RedisServer, BaseRedisAsyncCommands<String, String>> ASYNCS = new HashMap<>();
    protected static final Map<RedisServer, BaseRedisCommands<String, String>> SYNCS = new HashMap<>();

    @BeforeAll
    public static void setup() {
        add(REDIS, REDIS_CLUSTER);
    }

    private static void add(RedisServer... containers) {
        for (RedisServer container : containers) {
            if (container instanceof RedisClusterContainer) {
                RedisModulesClusterClient client = RedisModulesClusterClient.create(container.getRedisURI());
                CLIENTS.put(container, client);
                StatefulRedisClusterConnection<String, String> connection = client.connect();
                CONNECTIONS.put(container, connection);
                SYNCS.put(container, connection.sync());
                ASYNCS.put(container, connection.async());
                PUBSUB_CONNECTIONS.put(container, client.connectPubSub());
                POOLS.put(container, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
            } else {
                RedisModulesClient client = RedisModulesClient.create(container.getRedisURI());
                CLIENTS.put(container, client);
                StatefulRedisConnection<String, String> connection = client.connect();
                CONNECTIONS.put(container, connection);
                SYNCS.put(container, connection.sync());
                ASYNCS.put(container, connection.async());
                PUBSUB_CONNECTIONS.put(container, client.connectPubSub());
                POOLS.put(container, ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>()));
            }
        }
    }

    @AfterEach
    public void flushall() {
        for (BaseRedisCommands<String, String> sync : SYNCS.values()) {
            ((RedisServerCommands<String, String>) sync).flushall();
        }
    }

    @AfterAll
    public static void teardown() {
        CONNECTIONS.values().forEach(StatefulConnection::close);
        PUBSUB_CONNECTIONS.values().forEach(StatefulConnection::close);
        POOLS.values().forEach(GenericObjectPool::close);
        CLIENTS.values().forEach(AbstractRedisClient::shutdown);
        CLIENTS.values().forEach(c -> c.getResources().shutdown());
        SYNCS.clear();
        ASYNCS.clear();
        CONNECTIONS.clear();
        PUBSUB_CONNECTIONS.clear();
        POOLS.clear();
        CLIENTS.clear();
    }

    static Stream<RedisServer> containers() {
        return Stream.of(REDIS, REDIS_CLUSTER);
    }

    static Stream<RedisContainer> standaloneContainer() {
        return Stream.of(REDIS);
    }

    protected static <T> T sync(RedisServer container) {
        return (T) SYNCS.get(container);
    }

    protected static <T> T async(RedisServer container) {
        return (T) ASYNCS.get(container);
    }

    protected static <C extends StatefulConnection<String, String>> C connection(RedisServer container) {
        return (C) CONNECTIONS.get(container);
    }

    protected static <C extends StatefulConnection<String, String>> GenericObjectPool<C> pool(RedisServer container) {
        return (GenericObjectPool<C>) POOLS.get(container);
    }

    protected DataGeneratorBuilder dataGenerator(RedisServer container) {
        if (container.isCluster()) {
            return DataGenerator.client((RedisModulesClusterClient) CLIENTS.get(container));
        }
        return DataGenerator.client((RedisModulesClient) CLIENTS.get(container));
    }


}
