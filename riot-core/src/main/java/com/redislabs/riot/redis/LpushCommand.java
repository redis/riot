package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.RedisListItemWriter;
import org.springframework.batch.item.redis.RedisListItemWriter.RedisListItemWriterBuilder.PushDirection;

import picocli.CommandLine.Command;

@Command(name = "lpush")
public class LpushCommand extends AbstractCollectionCommand {

    @Override
    public RedisListItemWriter<Map<String, Object>> writer() throws Exception {
	return configure(RedisListItemWriter.<Map<String, Object>>builder().direction(PushDirection.LEFT)).build();
    }

}
