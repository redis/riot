package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine.Command;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "noop", description = "No operation: accepts input and does nothing")
public class NoopCommand extends AbstractRedisCommand<Map<String, Object>> {

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return CommandBuilder.<Map<String, Object>>noop().build();
    }

}
