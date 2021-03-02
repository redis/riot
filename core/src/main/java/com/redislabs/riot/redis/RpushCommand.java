package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine.Command;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "rpush", description = "Insert values at the tail of lists")
public class RpushCommand extends AbstractCollectionCommand {

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return configure(CommandBuilder.rpush()).build();
    }

}
