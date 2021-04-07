package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.RedisOperationBuilder;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "noop", description = "No operation: accepts input and does nothing")
public class NoopCommand extends AbstractRedisCommand<Map<String, Object>> {

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        RedisOperationBuilder.NoopBuilder<String, String, Map<String, Object>> builder = RedisOperationBuilder.noop();
        return builder.build();
    }

}
