package com.redislabs.riot.cli;

import com.redislabs.riot.redis.writer.map.Expire;
import com.redislabs.riot.redis.writer.map.Expire.ExpireBuilder;

import picocli.CommandLine.Option;

public class ExpireOptions {

	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long defaultTimeout = 60;
	@Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private String timeout;

	public ExpireBuilder builder() {
		return Expire.builder().defaultTimeout(defaultTimeout).timeout(timeout);
	}

}
