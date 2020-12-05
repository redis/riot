package com.redislabs.riot.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.NoOpItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Command;

@Command(name = "noop")
public class NoopCommand extends AbstractRedisCommand<Map<String, Object>> {

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception {
		return NoOpItemWriter.<Map<String, Object>>builder().client(client).poolConfig(poolConfig).build();
	}

}
