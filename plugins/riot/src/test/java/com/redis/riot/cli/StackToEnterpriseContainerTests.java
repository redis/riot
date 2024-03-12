package com.redis.riot.cli;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.test.AbstractTestBase;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledOnOs(OS.LINUX)
class StackToEnterpriseContainerTests extends AbstractIntegrationTests {

    private static final RedisStackContainer source = RedisContainerFactory.stack();

    private static final RedisEnterpriseContainer target = RedisContainerFactory.enterprise();

    @Override
    protected RedisStackContainer getRedisServer() {
        return source;
    }

    @Override
    protected RedisEnterpriseContainer getTargetRedisServer() {
        return target;
    }

    @Override
    protected DataType[] generatorDataTypes() {
        return AbstractTestBase.REDIS_MODULES_GENERATOR_TYPES;
    }

}
