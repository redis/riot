package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.HashItemWriter;

import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.Command;

@Command(name = "hmset")
public class HmsetCommand extends AbstractKeyCommand {

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client, RedisOptions redisOptions)
			throws Exception {
		return configure(HashItemWriter.<Map<String, Object>>builder().client(client)
				.poolConfig(redisOptions.poolConfig()).mapConverter(new MapFlattener<>(new ObjectToStringConverter())))
						.build();
	}

}
