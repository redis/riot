package com.redis.riot.cli.common;

import java.util.Optional;

import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.reader.KeyComparisonReadOperation;
import com.redis.spring.batch.reader.KeyspaceNotificationOrderingStrategy;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.step.FlushingChunkProvider;

import picocli.CommandLine.Option;

public class ReplicationOptions {

	@Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private ReplicationMode mode = ReplicationMode.SNAPSHOT;

	@Option(names = "--type", description = "Replication strategy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private ReplicationStrategy strategy = ReplicationStrategy.DUMP;

	@Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int notificationQueueCapacity = QueueOptions.DEFAULT_CAPACITY;

	@Option(names = "--event-order", description = "Keyspace notification ordering strategy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private KeyspaceNotificationOrderingStrategy notificationOrdering = KeyspaceNotificationOrderingStrategy.PRIORITY;

	@Option(names = "--no-verify", description = "Disable verifying target against source dataset after replication.")
	private boolean noVerify;

	@Option(names = "--key-process", description = "SpEL expression to transform each key.", paramLabel = "<exp>")
	private Optional<String> keyProcessor = Optional.empty();

	@Option(names = "--key-slot", description = "Key slot range filter for keyspace notifications.", paramLabel = "<range>")
	private Optional<IntRange> keySlot = Optional.empty();

	@Option(names = "--ttl-tolerance", description = "Max TTL difference to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long ttlTolerance = KeyComparisonReadOperation.DEFAULT_TTL_TOLERANCE.toMillis();

	@Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification.")
	private boolean showDiffs;

	@Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long flushInterval = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL.toMillis();

	@Option(names = "--idle-timeout", description = "Min duration of inactivity to consider transfer complete (default: no timeout).", paramLabel = "<ms>")
	private long idleTimeout;

	@Option(names = "--target-pool", description = "Max connections for target Redis pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int targetPoolMaxTotal = PoolOptions.DEFAULT_MAX_TOTAL;

	public KeyspaceNotificationOrderingStrategy getNotificationOrdering() {
		return notificationOrdering;
	}

	public void setNotificationOrdering(KeyspaceNotificationOrderingStrategy notificationOrdering) {
		this.notificationOrdering = notificationOrdering;
	}

	public int getTargetPoolMaxTotal() {
		return targetPoolMaxTotal;
	}

	public void setTargetPoolMaxTotal(int targetPoolMaxTotal) {
		this.targetPoolMaxTotal = targetPoolMaxTotal;
	}

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

	public boolean isShowDiffs() {
		return showDiffs;
	}

	public void setShowDiffs(boolean showDiffs) {
		this.showDiffs = showDiffs;
	}

}
