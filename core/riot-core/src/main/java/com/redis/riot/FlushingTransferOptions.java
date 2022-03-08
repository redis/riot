package com.redis.riot;

import java.time.Duration;
import java.util.OptionalLong;

import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.util.Assert;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.reader.LiveRedisItemReaderBuilder;
import com.redis.spring.batch.step.FlushingSimpleStepBuilder;

import picocli.CommandLine;

public class FlushingTransferOptions {

	@CommandLine.Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long flushInterval = 50;
	@CommandLine.Option(names = "--idle-timeout", description = "Min duration of inactivity to consider transfer complete", paramLabel = "<ms>")
	private OptionalLong idleTimeout = OptionalLong.empty();

	public void setFlushInterval(Duration flushInterval) {
		Assert.notNull(flushInterval, "Flush interval must not be null");
		this.flushInterval = flushInterval.toMillis();
	}

	public void setIdleTimeout(Duration idleTimeoutDuration) {
		Assert.notNull(idleTimeoutDuration, "Duration must not be null");
		this.idleTimeout = OptionalLong.of(idleTimeoutDuration.toMillis());
	}

	private Duration getFlushInterval() {
		return Duration.ofMillis(flushInterval);
	}

	public <I, O> FlushingSimpleStepBuilder<I, O> configure(FaultTolerantStepBuilder<I, O> step) {
		FlushingSimpleStepBuilder<I, O> builder = new FlushingSimpleStepBuilder<>(step)
				.flushingInterval(getFlushInterval());
		idleTimeout.ifPresent(t -> builder.idleTimeout(Duration.ofMillis(t)));
		return builder;
	}

	public <K, V, T extends KeyValue<K, ?>> LiveRedisItemReaderBuilder<K, V, T> configure(
			LiveRedisItemReaderBuilder<K, V, T> reader) {
		reader.flushingInterval(getFlushInterval());
		idleTimeout.ifPresent(t -> reader.idleTimeout(Duration.ofMillis(t)));
		return reader;
	}

}
