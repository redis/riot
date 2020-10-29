package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.RedisSetItemWriter;

import picocli.CommandLine.Command;

@Command(name = "sadd")
public class SaddCommand extends AbstractCollectionCommand {

	@Override
	public RedisSetItemWriter<String, String, Map<String, Object>> writer() throws Exception {
		return configure(RedisSetItemWriter.<Map<String, Object>>builder()).build();
	}

}
