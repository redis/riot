package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.redis.writer.map.Expire;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractRedisCommand {

	@Option(names = "--default-timeout", description = "Default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long defaultTimeout = 60;
	@Option(names = "--timeout", description = "Field to get the timeout value from", paramLabel = "<f>")
	private String timeout;

	@Override
	protected Expire redisWriter() {
		Expire writer = new Expire();
		writer.defaultTimeout(defaultTimeout);
		writer.timeoutField(timeout);
		return writer;
	}

}
