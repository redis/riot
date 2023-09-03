package com.redis.riot.core.file;

import com.redis.riot.core.RedisContainerFactory;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class StackFileTests extends FileTests {

    private static final RedisStackContainer redis = RedisContainerFactory.stack();

    @Override
    protected RedisServer getRedisServer() {
        return redis;
    }

}
