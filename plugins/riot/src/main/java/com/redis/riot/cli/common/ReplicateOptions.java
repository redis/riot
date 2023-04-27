package com.redis.riot.cli.common;

import java.util.Optional;

import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.reader.QueueOptions;

import picocli.CommandLine.Option;

public class ReplicateOptions {

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

}
