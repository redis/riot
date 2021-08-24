package com.redis.riot.redis;

import org.springframework.batch.item.redis.support.operation.Noop;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "noop", description = "No operation: accepts input and does nothing")
public class NoopCommand extends AbstractRedisCommand<Map<String, Object>> {

    @Override
    public Noop<String, String, Map<String, Object>> operation() {
        return new Noop<>();
    }

}
