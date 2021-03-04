package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "hset", aliases = "hmset", description = "Set hashes from input")
public class HsetCommand extends AbstractKeyCommand {

    @CommandLine.Mixin
    private FilteringOptions filtering = FilteringOptions.builder().build();

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return configureKeyCommandBuilder(CommandBuilder.hset()).mapConverter(filtering.converter()).build();
    }

}
