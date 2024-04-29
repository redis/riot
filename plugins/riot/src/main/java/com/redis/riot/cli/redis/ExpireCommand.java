package com.redis.riot.cli.redis;

import java.time.Duration;

import com.redis.riot.core.operation.ExpireBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractWriteOperationCommand {

	public static final long DEFAULT_TTL = 60;

	@Option(names = "--ttl", description = "EXPIRE timeout field.", paramLabel = "<field>")
	private String ttlField;

	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long defaultTtl = DEFAULT_TTL;

	@Override
	protected ExpireBuilder operationBuilder() {
		ExpireBuilder builder = new ExpireBuilder();
		builder.setTtlField(ttlField);
		builder.setDefaultTtl(Duration.ofSeconds(defaultTtl));
		return builder;
	}

}