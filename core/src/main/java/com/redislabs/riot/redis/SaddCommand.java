package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.RedisOperation;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractCollectionCommand {

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return configureCollectionCommandBuilder(RedisOperation.sadd()).build();
    }

}
