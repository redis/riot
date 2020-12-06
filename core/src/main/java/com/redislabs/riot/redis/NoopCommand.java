package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.NoOpItemWriter;

import com.redislabs.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.Command;

@Command(name = "noop", description = "No operation: accepts input and does nothing")
public class NoopCommand extends AbstractRedisCommand<Map<String, Object>> {

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client, RedisOptions redisOptions)
			throws Exception {
		return NoOpItemWriter.<Map<String, Object>>builder().client(client).poolConfig(redisOptions.poolConfig())
				.build();
	}

}
