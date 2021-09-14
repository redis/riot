package com.redis.riot.redis;

import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.operation.Sadd;
import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "sadd", description = "Add members to a set")
public class SaddCommand extends AbstractCollectionCommand {

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return Sadd.key(key()).member(member()).build();
    }

}
