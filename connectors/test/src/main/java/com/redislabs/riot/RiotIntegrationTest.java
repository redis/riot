package com.redislabs.riot;

import com.redislabs.testcontainers.RedisClusterContainer;
import com.redislabs.testcontainers.RedisContainer;
import com.redislabs.testcontainers.RedisStandaloneContainer;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Testcontainers
public abstract class RiotIntegrationTest extends RiotTest {

    @Container
    private static final RedisStandaloneContainer REDIS = new RedisStandaloneContainer().withKeyspaceNotifications();
    @Container
    private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer().withKeyspaceNotifications();

    protected final Map<RedisContainer, AbstractRedisClient> clients = new HashMap<>();
    protected final Map<RedisContainer, StatefulConnection<String, String>> connections = new HashMap<>();
    protected final Map<RedisContainer, StatefulRedisPubSubConnection<String, String>> pubSubConnections = new HashMap<>();
    protected final Map<RedisContainer, BaseRedisAsyncCommands<String, String>> asyncs = new HashMap<>();
    protected final Map<RedisContainer, BaseRedisCommands<String, String>> syncs = new HashMap<>();

    @BeforeEach
    public void setupEach() {
        add(REDIS);
        add(REDIS_CLUSTER);
    }

    private void add(RedisContainer container) {
        String uri = container.getRedisURI();
        if (container instanceof RedisClusterContainer) {
            RedisClusterClient client = RedisClusterClient.create(uri);
            clients.put(container, client);
            StatefulRedisClusterConnection<String, String> connection = client.connect();
            connections.put(container, connection);
            syncs.put(container, connection.sync());
            asyncs.put(container, connection.async());
            pubSubConnections.put(container, client.connectPubSub());
            return;
        }
        RedisClient client = RedisClient.create(uri);
        clients.put(container, client);
        StatefulRedisConnection<String, String> connection = client.connect();
        connections.put(container, connection);
        syncs.put(container, connection.sync());
        asyncs.put(container, connection.async());
        pubSubConnections.put(container, client.connectPubSub());
    }

    @AfterEach
    public void cleanupEach() {
        for (BaseRedisCommands<String, String> sync : syncs.values()) {
            ((RedisServerCommands<String, String>) sync).flushall();
        }
        for (StatefulConnection<String, String> connection : connections.values()) {
            connection.close();
        }
        for (StatefulRedisPubSubConnection<String, String> connection : pubSubConnections.values()) {
            connection.close();
        }
        for (AbstractRedisClient client : clients.values()) {
            client.shutdown();
            client.getResources().shutdown();
        }
        syncs.clear();
        asyncs.clear();
        connections.clear();
        pubSubConnections.clear();
        clients.clear();
    }

    static Stream<RedisContainer> containers() {
        return Stream.of(REDIS, REDIS_CLUSTER);
    }

    protected <T> T sync(RedisContainer container) {
        return (T) syncs.get(container);
    }

    protected <T> T async(RedisContainer container) {
        return (T) asyncs.get(container);
    }

    protected <C extends StatefulConnection<String, String>> C connection(RedisContainer container) {
        return (C) connections.get(container);
    }

    protected <C extends StatefulConnection<String, String>> GenericObjectPool<C> pool(RedisContainer container) {
        GenericObjectPoolConfig<StatefulConnection<String, String>> config = new GenericObjectPoolConfig<>();
        if (container instanceof RedisClusterContainer) {
            return (GenericObjectPool<C>) ConnectionPoolSupport.createGenericObjectPool(((RedisClusterClient) clients.get(container))::connect, config);
        }
        return (GenericObjectPool<C>) ConnectionPoolSupport.createGenericObjectPool(((RedisClient) clients.get(container))::connect, config);
    }

    protected DataGenerator.DataGeneratorBuilder dataGenerator(RedisContainer container) {
        return DataGenerator.builder(connection(container));
    }


}
