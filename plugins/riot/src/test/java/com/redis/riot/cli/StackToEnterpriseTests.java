package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.test.AbstractTestBase;
import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class StackToEnterpriseTests extends IntegrationTests {

    private static final RedisServer SOURCE = RedisContainerFactory.stack();

    private static final RedisServer TARGET = RedisContainerFactory.enterprise();

    @Override
    protected RedisServer getRedisServer() {
        return SOURCE;
    }

    @Override
    protected RedisServer getTargetRedisServer() {
        return TARGET;
    }

    @Override
    protected DataType[] generatorDataTypes() {
        return AbstractTestBase.REDIS_MODULES_GENERATOR_TYPES;
    }

}
