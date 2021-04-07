package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.RedisOperationBuilder;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "lpush", description = "Insert values at the head of a list")
public class LpushCommand extends AbstractCollectionCommand {

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return configureCollectionCommandBuilder(RedisOperationBuilder.lpush()).build();
    }

}
