package com.redis.riot.cli.redis;

import com.redis.riot.core.operation.ExpireBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractWriteOperationCommand {

	public static final long DEFAULT_TTL = 60;

	@Option(names = "--ttl", description = "EXPIRE timeout field.", paramLabel = "<field>")
	private String ttlField;

	@Override
	protected ExpireBuilder operationBuilder() {
		ExpireBuilder builder = new ExpireBuilder();
		builder.ttlField(ttlField);
		return builder;
	}

}