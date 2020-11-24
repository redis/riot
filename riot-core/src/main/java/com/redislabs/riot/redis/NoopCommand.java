package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.RedisNoOpItemWriter;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;

import picocli.CommandLine.Command;

@Command(name = "noop")
public class NoopCommand extends AbstractRedisCommand<Map<String, Object>> {

	@Override
	public AbstractRedisItemWriter<Map<String, Object>> writer() throws Exception {
		return configure(RedisNoOpItemWriter.<Map<String, Object>>builder()).build();
	}

}
