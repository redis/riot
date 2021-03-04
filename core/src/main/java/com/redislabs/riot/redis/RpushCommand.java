package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine.Command;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "rpush", description = "Insert values at the tail of a list")
public class RpushCommand extends AbstractCollectionCommand {

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return configureCollectionCommandBuilder(CommandBuilder.rpush()).build();
    }

}
