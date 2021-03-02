package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine.Command;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "sadd", description = "Add members to sets")
public class SaddCommand extends AbstractCollectionCommand {

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return configure(CommandBuilder.sadd()).build();
    }

}
