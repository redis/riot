package com.redis.riot.cli.common;

import java.time.Duration;
import java.util.Optional;

import com.redis.spring.batch.RedisItemReader.ComparatorBuilder;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.step.FlushingChunkProvider;

import picocli.CommandLine.Option;

public class ReplicationOptions {

	public enum ReplicationMode {
		SNAPSHOT, LIVE, LIVEONLY, COMPARE
	}

	public enum ReplicationStrategy {
		DUMP, DS
	}

	@Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private ReplicationMode mode = ReplicationMode.SNAPSHOT;

	@Option(names = "--type", description = "Replication strategy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private ReplicationStrategy strategy = ReplicationStrategy.DUMP;

	@Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int notificationQueueCapacity = QueueOptions.DEFAULT_CAPACITY;

	@Option(names = "--no-verify", description = "Disable verifying target against source dataset after replication.")
	private boolean noVerify;

	@Option(names = "--key-process", description = "SpEL expression to transform each key.", paramLabel = "<exp>")
	private Optional<String> keyProcessor = Optional.empty();

	@Option(names = "--key-slot", description = "Key slot range filter for keyspace notifications.", paramLabel = "<range>")
	private Optional<IntRange> keySlot = Optional.empty();

	@Option(names = "--ttl-tolerance", description = "Max TTL difference to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<millis>")
	private long ttlTolerance = ComparatorBuilder.DEFAULT_TTL_TOLERANCE.toMillis();

	@Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification.")
	private boolean showDiffs;

	@Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long flushInterval = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL.toMillis();

	@Option(names = "--idle-timeout", description = "Min duration of inactivity to consider transfer complete.", paramLabel = "<ms>")
	private long idleTimeout;

	public long getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(long millis) {
		this.flushInterval = millis;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long millis) {
		this.idleTimeout = millis;
	}

	public ReplicationMode getMode() {
		return mode;
	}

	public void setMode(ReplicationMode mode) {
		this.mode = mode;
	}

	public ReplicationStrategy getStrategy() {
		return strategy;
	}

	public void setStrategy(ReplicationStrategy strategy) {
		this.strategy = strategy;
	}

	public Optional<IntRange> getKeySlot() {
		return keySlot;
	}

	public void setKeySlot(Optional<IntRange> keySlot) {
		this.keySlot = keySlot;
	}

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public void setNotificationQueueCapacity(int notificationQueueCapacity) {
		this.notificationQueueCapacity = notificationQueueCapacity;
	}

	public boolean isNoVerify() {
		return noVerify;
	}

	public void setNoVerify(boolean noVerify) {
		this.noVerify = noVerify;
	}

	public Optional<String> getKeyProcessor() {
		return keyProcessor;
	}

	public void setKeyProcessor(Optional<String> keyProcessor) {
		this.keyProcessor = keyProcessor;
	}

	public QueueOptions notificationQueueOptions() {
		return QueueOptions.builder().capacity(notificationQueueCapacity).build();
	}

	public long getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(long ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	public Duration getTtlToleranceDuration() {
		return Duration.ofMillis(ttlTolerance);
	}

	public boolean isShowDiffs() {
		return showDiffs;
	}

	public void setShowDiffs(boolean showDiffs) {
		this.showDiffs = showDiffs;
	}

}
