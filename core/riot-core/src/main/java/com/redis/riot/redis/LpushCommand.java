package com.redis.riot.redis;

import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.support.operation.Lpush;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "lpush", description = "Insert values at the head of a list")
public class LpushCommand extends AbstractCollectionCommand {

    @Override
    public OperationItemWriter.RedisOperation<String, String, Map<String, Object>> operation() {
        return new Lpush<>(key(), member());
    }

}
