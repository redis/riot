package com.redis.riot.core;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class StackTests extends ReplicationTests {

    public static final RedisStackContainer SOURCE = RedisContainerFactory.stack();

    public static final RedisStackContainer TARGET = RedisContainerFactory.stack();

    @Override
    protected RedisServer getRedisServer() {
        return SOURCE;
    }

    @Override
    protected RedisServer getTargetRedisServer() {
        return TARGET;
    }

}
