package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.ExpireItemWriter;

import com.redislabs.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description="Set timeouts on keys")
public class ExpireCommand extends AbstractKeyCommand {

	@Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private String timeoutField;
	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeoutDefault = 60;

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client, RedisOptions redisOptions)
			throws Exception {
		return configure(
				ExpireItemWriter.<Map<String, Object>>builder().client(client).poolConfig(redisOptions.poolConfig())
						.timeoutConverter(numberFieldExtractor(Long.class, timeoutField, timeoutDefault))).build();
	}

}
