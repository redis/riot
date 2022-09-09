package com.redis.riot;

import java.time.Duration;
import java.util.Optional;

import org.springframework.util.Assert;

import com.redis.spring.batch.step.FlushingOptions;

import picocli.CommandLine.Option;

public class FlushingTransferOptions {

	@Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long flushInterval = FlushingOptions.DEFAULT_FLUSHING_INTERVAL.toMillis();
	@Option(names = "--idle-timeout", description = "Min duration of inactivity to consider transfer complete.", paramLabel = "<ms>")
	private Optional<Long> idleTimeout = Optional.empty();

	public void setFlushInterval(Duration flushInterval) {
		Assert.notNull(flushInterval, "Flush interval must not be null");
		this.flushInterval = flushInterval.toMillis();
	}

	public void setIdleTimeout(Duration idleTimeoutDuration) {
		Assert.notNull(idleTimeoutDuration, "Duration must not be null");
		this.idleTimeout = Optional.of(idleTimeoutDuration.toMillis());
	}

	@Override
	public String toString() {
		return "FlushingTransferOptions [flushInterval=" + flushInterval + ", idleTimeout=" + idleTimeout + "]";
	}

	public FlushingOptions flushingOptions() {
		return FlushingOptions.builder().interval(Duration.ofMillis(flushInterval))
				.timeout(idleTimeout.map(Duration::ofMillis)).build();
	}

}
