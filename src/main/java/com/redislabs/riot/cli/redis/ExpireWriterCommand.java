package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.ExpireWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Redis expiration")
public class ExpireWriterCommand extends AbstractDataStructureWriterCommand {

	@Option(names = "--default-timeout", description = "Default timeout in seconds", paramLabel = "<seconds>")
	private long defaultTimeout = 60;
	@Option(names = "--timeout", description = "Field to get the timeout value from", paramLabel = "<field>")
	private String timeoutField;

	public ExpireWriter writer() {
		return new ExpireWriter(timeoutField, defaultTimeout);
	}

}
