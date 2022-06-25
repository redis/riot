package com.redis.riot.redis;

import java.util.Map;
import java.util.Optional;

import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.operation.Expire;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractKeyCommand {

	@Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private Optional<String> timeoutField = Optional.empty();
	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeoutDefault = 60;

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Expire.<String, String, Map<String, Object>>key(key())
				.millis(numberExtractor(timeoutField, Long.class, timeoutDefault)).build();
	}

}
