package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine.Command;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractCollectionCommand {

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return configureCollectionCommandBuilder(CommandBuilder.sadd()).build();
    }

}
