package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.SetItemWriter;

import com.redislabs.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.Command;

@Command(name = "sadd")
public class SaddCommand extends AbstractCollectionCommand {

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client, RedisOptions redisOptions)
			throws Exception {
		return configure(
				SetItemWriter.<Map<String, Object>>builder().client(client).poolConfig(redisOptions.poolConfig()))
						.build();
	}

}
