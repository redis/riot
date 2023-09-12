package com.redis.riot.core;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class ReplicationExecutionContext extends RiotExecutionContext {

    private final RiotExecutionContext targetExecutionContext;

    public ReplicationExecutionContext(RedisURI redisURI, AbstractRedisClient redisClient, RedisURI targetRedisURI,
            AbstractRedisClient targetRedisClient) {
        super(redisURI, redisClient);
        this.targetExecutionContext = new RiotExecutionContext(targetRedisURI, targetRedisClient);
    }

    public ReplicationExecutionContext(RedisOptions redisClientOptions, RedisOptions targetRedisClientOptions) {
        super(redisClientOptions);
        this.targetExecutionContext = new RiotExecutionContext(targetRedisClientOptions);
    }

    public RiotExecutionContext getTargetExecutionContext() {
        return targetExecutionContext;
    }

    @Override
    public void close() {
        try {
            targetExecutionContext.close();
        } finally {
            super.close();
        }
    }

}
