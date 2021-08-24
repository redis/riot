package com.redis.riot.redis;

import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.support.operation.Rpush;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "rpush", description = "Insert values at the tail of a list")
public class RpushCommand extends AbstractCollectionCommand {

    @Override
    public OperationItemWriter.RedisOperation<String, String, Map<String, Object>> operation() {
        return new Rpush<>(key(), member(), t -> false, t -> false);
    }

}
