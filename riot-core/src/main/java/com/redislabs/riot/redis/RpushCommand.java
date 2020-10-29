package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.RedisListItemWriter;
import org.springframework.batch.item.redis.RedisListItemWriter.RedisListItemWriterBuilder.PushDirection;

import picocli.CommandLine.Command;

@Command(name = "rpush")
public class RpushCommand extends AbstractCollectionCommand {

	@Override
	public RedisListItemWriter<String, String, Map<String, Object>> writer() throws Exception {
		return configure(RedisListItemWriter.<Map<String, Object>>builder().direction(PushDirection.RIGHT)).build();
	}

}
