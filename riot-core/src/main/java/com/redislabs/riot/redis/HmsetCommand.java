package com.redislabs.riot.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.HashItemWriter;

import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Command;

@Command(name = "hmset")
public class HmsetCommand extends AbstractKeyCommand {

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception {
		return configure(HashItemWriter.<Map<String, Object>>builder().client(client).poolConfig(poolConfig)
				.mapConverter(new MapFlattener<>(new ObjectToStringConverter()))).build();
	}

}
