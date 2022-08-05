package com.redis.riot.redis;

import java.util.Optional;

import picocli.CommandLine.Option;

public class ExpireOptions {

	@Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private Optional<String> timeoutField = Optional.empty();
	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeoutDefault = 60;

	public Optional<String> getTimeoutField() {
		return timeoutField;
	}

	public void setTimeoutField(Optional<String> timeoutField) {
		this.timeoutField = timeoutField;
	}

	public long getTimeoutDefault() {
		return timeoutDefault;
	}

	public void setTimeoutDefault(long timeoutDefault) {
		this.timeoutDefault = timeoutDefault;
	}

	@Override
	public String toString() {
		return "ExpireOptions [timeoutField=" + timeoutField + ", timeoutDefault=" + timeoutDefault + "]";
	}

}
