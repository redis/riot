package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.Optional;

import com.redis.spring.batch.writer.operation.Expire;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractKeyCommand {

	public static final long DEFAULT_TTL = 60;

	@Option(names = "--ttl", description = "EXPIRE timeout field.", paramLabel = "<field>")
	private Optional<String> ttlField = Optional.empty();

	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long defaultTtl = DEFAULT_TTL;

	@Override
	public Expire<String, String, Map<String, Object>> operation() {
		return new Expire<>(key(), longExtractor(ttlField, defaultTtl));
	}

}
