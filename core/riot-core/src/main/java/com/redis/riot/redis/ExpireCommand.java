package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.operation.Expire;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractKeyCommand {

	@Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private String timeoutField;
	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeoutDefault = 60;

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Expire.<String, String, Map<String, Object>>key(key())
				.millis(numberExtractor(timeoutField, Long.class, timeoutDefault)).build();
	}

}
