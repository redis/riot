package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.operation.Hset;

import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "hset", aliases = "hmset", description = "Set hashes from input")
public class HsetCommand extends AbstractKeyCommand {

    @CommandLine.Mixin
    private FilteringOptions filtering = new FilteringOptions();

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return Hset.key(key()).map(filtering.converter()).build();
    }

}
