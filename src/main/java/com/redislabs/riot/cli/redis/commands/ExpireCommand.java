package com.redislabs.riot.cli.redis.commands;

import com.redislabs.riot.batch.redis.writer.Expire;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractRedisCommand {

	@Option(names = "--default-timeout", description = "Default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long defaultTimeout = 60;
	@Option(names = "--timeout", description = "Field to get the timeout value from", paramLabel = "<f>")
	private String timeout;

	@SuppressWarnings("rawtypes")
	@Override
	protected Expire writer() {
		return new Expire().defaultTimeout(defaultTimeout).timeoutField(timeout);
	}

}
