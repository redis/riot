package com.redis.riot;

import java.time.Duration;

import picocli.CommandLine;

public class FlushingTransferOptions {

	@CommandLine.Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long flushInterval = 50;
	@CommandLine.Option(names = "--idle-timeout", description = "Min duration of inactivity to consider transfer complete", paramLabel = "<ms>")
	private Long idleTimeout;

	public Duration getFlushIntervalDuration() {
		return Duration.ofMillis(flushInterval);
	}

	public long getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(long flushInterval) {
		this.flushInterval = flushInterval;
	}

	public void setIdleTimeout(Duration idleTimeoutDuration) {
		if (idleTimeoutDuration == null) {
			this.idleTimeout = null;
		} else {
			this.idleTimeout = idleTimeoutDuration.toMillis();
		}
	}

	public Duration getIdleTimeoutDuration() {
		if (idleTimeout == null) {
			return null;
		}
		return Duration.ofMillis(idleTimeout);
	}

}
