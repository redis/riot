package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.operation.Expire;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractKeyCommand {

	@Mixin
	private ExpireOptions options = new ExpireOptions();

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Expire.<String, String, Map<String, Object>>key(key())
				.millis(numberExtractor(options.getTimeoutField(), Long.class, options.getTimeoutDefault())).build();
	}

}
