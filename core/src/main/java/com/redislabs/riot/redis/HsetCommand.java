package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.RedisOperation;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "hset", aliases = "hmset", description = "Set hashes from input")
public class HsetCommand extends AbstractKeyCommand {

    @CommandLine.Mixin
    private FilteringOptions filtering = FilteringOptions.builder().build();

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return configureKeyCommandBuilder(RedisOperation.hset()).map(filtering.converter()).build();
    }

}
