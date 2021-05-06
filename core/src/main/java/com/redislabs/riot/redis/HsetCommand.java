package com.redislabs.riot.redis;

import lombok.Getter;
import lombok.Setter;
import org.springframework.batch.item.redis.RedisOperation;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "hset", aliases = "hmset", description = "Set hashes from input")
public class HsetCommand extends AbstractKeyCommand {

    @Setter
    @Getter
    @CommandLine.Mixin
    private FilteringOptions filtering = new FilteringOptions();

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return configureKeyCommandBuilder(RedisOperation.hset()).map(filtering.converter()).build();
    }

}
