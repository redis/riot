package com.redislabs.riot.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.support.operation.Hset;
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
    public OperationItemWriter.RedisOperation<String, String, Map<String, Object>> operation() {
        return new Hset<>(key(), filtering.converter());
    }

}
