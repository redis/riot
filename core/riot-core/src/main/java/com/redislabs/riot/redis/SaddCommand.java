package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.support.operation.Sadd;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractCollectionCommand {

    @Override
    public OperationItemWriter.RedisOperation<Map<String, Object>> operation() {
        return new Sadd<>(key(), member());
    }

}
