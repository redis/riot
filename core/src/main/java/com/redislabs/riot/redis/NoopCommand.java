package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.NoOpItemWriter;

import com.redislabs.riot.TransferContext;

import picocli.CommandLine.Command;

@Command(name = "noop", aliases = "n", description = "No operation: accepts input and does nothing")
public class NoopCommand extends AbstractRedisCommand<Map<String, Object>> {

	@Override
	public ItemWriter<Map<String, Object>> writer(TransferContext context) throws Exception {
		return NoOpItemWriter.<Map<String, Object>>builder(context.getClient())
				.poolConfig(context.getRedisOptions().poolConfig()).build();
	}

}
