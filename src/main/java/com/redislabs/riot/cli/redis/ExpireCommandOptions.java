package com.redislabs.riot.cli.redis;

import com.redislabs.riot.batch.redis.writer.ExpireMapWriter;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class ExpireCommandOptions {

	@Option(names = "--expire-default", description = "Default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long expireDefaultTimeout = 60;
	@Option(names = "--expire-timeout", description = "Field to get the timeout value from", paramLabel = "<f>")
	private String expireTimeout;

	public ExpireMapWriter writer() {
		ExpireMapWriter expireWriter = new ExpireMapWriter();
		expireWriter.setDefaultTimeout(expireDefaultTimeout);
		expireWriter.setTimeoutField(expireTimeout);
		return expireWriter;
	}
}
