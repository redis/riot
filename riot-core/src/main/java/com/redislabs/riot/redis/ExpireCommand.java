package com.redislabs.riot.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.ExpireItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire")
public class ExpireCommand extends AbstractKeyCommand {

	@Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private String timeoutField;
	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeoutDefault = 60;

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception {
		return configure(ExpireItemWriter.<Map<String, Object>>builder().client(client).poolConfig(poolConfig)
				.timeoutConverter(numberFieldExtractor(Long.class, timeoutField, timeoutDefault))).build();
	}

}
