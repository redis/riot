package com.redis.riot.core;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class RedisContext implements AutoCloseable {

    private final RedisURI uri;

    private final AbstractRedisClient client;

    private final StatefulRedisModulesConnection<String, String> connection;

    public RedisContext(RedisURI uri, AbstractRedisClient client) {
        this.uri = uri;
        this.client = client;
        this.connection = RedisModulesUtils.connection(client);
    }

    public AbstractRedisClient getClient() {
        return client;
    }

    public StatefulRedisModulesConnection<String, String> getConnection() {
        return connection;
    }

    public RedisURI getUri() {
        return uri;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } finally {
            client.close();
            client.getResources().shutdown();
        }
    }

}
