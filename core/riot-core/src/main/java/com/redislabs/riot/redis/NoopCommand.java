package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.RedisOperation;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "noop", description = "No operation: accepts input and does nothing")
public class NoopCommand extends AbstractRedisCommand<Map<String, Object>> {

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return RedisOperation.noop();
    }

}
