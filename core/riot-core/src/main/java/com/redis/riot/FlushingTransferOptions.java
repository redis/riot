package com.redis.riot;

import java.time.Duration;
import java.util.Optional;

import org.springframework.util.Assert;

import com.redis.spring.batch.common.FlushingOptions;
import com.redis.spring.batch.step.FlushingChunkProvider;

import picocli.CommandLine.Option;

public class FlushingTransferOptions {

	@Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long flushInterval = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL.toMillis();
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
		return FlushingOptions.builder().flushingInterval(Duration.ofMillis(flushInterval))
				.idleTimeout(idleTimeout.map(Duration::ofMillis)).build();
	}

}
