package com.redis.riot.cli.operation;

import java.util.Optional;

import picocli.CommandLine.Option;

public class ExpireOptions {

	public static final long DEFAULT_TTL = 60;

	@Option(names = "--ttl", description = "EXPIRE timeout field.", paramLabel = "<field>")
	private Optional<String> ttlField = Optional.empty();

	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long defaultTtl = DEFAULT_TTL;

	public Optional<String> getTtlField() {
		return ttlField;
	}

	public void setTtlField(Optional<String> field) {
		this.ttlField = field;
	}

	public long getDefaultTtl() {
		return defaultTtl;
	}

	public void setDefaultTtl(long ttl) {
		this.defaultTtl = ttl;
	}

	@Override
	public String toString() {
		return "ExpireOptions [ttlField=" + ttlField + ", defaultTtl=" + defaultTtl + "]";
	}

}
