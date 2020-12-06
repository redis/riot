package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.ListItemWriter;
import org.springframework.batch.item.redis.ListItemWriter.ListItemWriterBuilder.PushDirection;

import com.redislabs.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.Command;

@Command(name = "rpush")
public class RpushCommand extends AbstractCollectionCommand {

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client, RedisOptions redisOptions)
			throws Exception {
		return configure(ListItemWriter.<Map<String, Object>>builder().client(client)
				.poolConfig(redisOptions.poolConfig()).direction(PushDirection.RIGHT)).build();
	}

}
