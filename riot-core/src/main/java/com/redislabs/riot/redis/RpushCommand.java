package com.redislabs.riot.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.ListItemWriter;
import org.springframework.batch.item.redis.ListItemWriter.ListItemWriterBuilder.PushDirection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Command;

@Command(name = "rpush")
public class RpushCommand extends AbstractCollectionCommand {

	
	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception {
		return configure(ListItemWriter.<Map<String, Object>>builder().client(client)
				.poolConfig(poolConfig).direction(PushDirection.RIGHT)).build();
	}

}
