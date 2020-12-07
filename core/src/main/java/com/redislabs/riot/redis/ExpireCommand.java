package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.ExpireItemWriter;

import com.redislabs.riot.TransferContext;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", aliases = "ex", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractKeyCommand {

	@Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private String timeoutField;
	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeoutDefault = 60;

	@Override
	public ItemWriter<Map<String, Object>> writer(TransferContext context) throws Exception {
		return configure(ExpireItemWriter.<Map<String, Object>>builder(context.getClient())
				.poolConfig(context.getRedisOptions().poolConfig())
				.timeoutConverter(numberFieldExtractor(Long.class, timeoutField, timeoutDefault))).build();
	}

}
