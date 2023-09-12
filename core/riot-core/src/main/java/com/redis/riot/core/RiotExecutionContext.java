package com.redis.riot.core;

import java.util.function.Function;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class RiotExecutionContext implements AutoCloseable {

    private final RedisURI redisURI;

    private final AbstractRedisClient redisClient;

    private final StatefulRedisModulesConnection<String, String> redisConnection;

    public RiotExecutionContext(RedisOptions redisClientOptions) {
        this(redisClientOptions.getUriOptions().redisURI(), redisClientOptions::client);
    }

    private RiotExecutionContext(RedisURI redisURI, Function<RedisURI, AbstractRedisClient> redisClient) {
        this(redisURI, redisClient.apply(redisURI));
    }

    public RiotExecutionContext(RedisURI redisURI, AbstractRedisClient redisClient) {
        this.redisURI = redisURI;
        this.redisClient = redisClient;
        this.redisConnection = RedisModulesUtils.connection(redisClient);
    }

    public RedisURI getRedisURI() {
        return redisURI;
    }

    public AbstractRedisClient getRedisClient() {
        return redisClient;
    }

    public StatefulRedisModulesConnection<String, String> getRedisConnection() {
        return redisConnection;
    }

    @Override
    public void close() {
        try {
            redisConnection.close();
        } finally {
            redisClient.close();
            redisClient.getResources().shutdown();
        }
    }

}
