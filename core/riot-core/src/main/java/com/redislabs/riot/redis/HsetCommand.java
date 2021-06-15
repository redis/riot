package com.redislabs.riot.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.batch.item.redis.RedisOperation;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "hset", aliases = "hmset", description = "Set hashes from input")
public class HsetCommand extends AbstractKeyCommand {

    @CommandLine.Mixin
    private FilteringOptions filtering = new FilteringOptions();

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return configureKeyCommandBuilder(RedisOperation.hset()).map(filtering.converter()).build();
    }

}
