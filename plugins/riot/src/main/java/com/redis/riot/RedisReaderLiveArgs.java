package com.redis.riot;

import java.time.temporal.ChronoUnit;

import com.redis.riot.core.RiotDuration;
import com.redis.spring.batch.item.redis.RedisItemReader;

import picocli.CommandLine.Option;

public class RedisReaderLiveArgs {

	public static final int DEFAULT_EVENT_QUEUE_CAPACITY = RedisItemReader.DEFAULT_EVENT_QUEUE_CAPACITY;
	public static final RiotDuration DEFAULT_FLUSH_INTERVAL = RiotDuration.of(RedisItemReader.DEFAULT_FLUSH_INTERVAL,
			ChronoUnit.MILLIS);

	@Option(names = "--flush-interval", description = "Max duration between flushes in live mode (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
	private RiotDuration flushInterval = DEFAULT_FLUSH_INTERVAL;

	@Option(names = "--idle-timeout", description = "Min duration to consider reader complete in live mode, for example 3s 5m (default: no timeout).", paramLabel = "<dur>")
	private RiotDuration idleTimeout;

	@Option(names = "--event-queue", description = "Capacity of the keyspace notification queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int eventQueueCapacity = DEFAULT_EVENT_QUEUE_CAPACITY;

	public <K> void configure(RedisItemReader<K, ?> reader) {
		reader.setFlushInterval(flushInterval.getValue());
		if (idleTimeout != null) {
			reader.setIdleTimeout(idleTimeout.getValue());
		}
		reader.setEventQueueCapacity(eventQueueCapacity);
	}

	public RiotDuration getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(RiotDuration interval) {
		this.flushInterval = interval;
	}

	public RiotDuration getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(RiotDuration idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public int getEventQueueCapacity() {
		return eventQueueCapacity;
	}

	public void setEventQueueCapacity(int capacity) {
		this.eventQueueCapacity = capacity;
	}
}