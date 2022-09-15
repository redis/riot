package com.redis.riot.redis;

import java.util.Optional;

import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.reader.QueueOptions;

import picocli.CommandLine.Option;

public class ReplicationOptions {

	public enum ReplicationMode {
		SNAPSHOT, LIVE, LIVEONLY
	}

	@Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private ReplicationMode mode = ReplicationMode.SNAPSHOT;

	@Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int notificationQueueCapacity = QueueOptions.DEFAULT_CAPACITY;

	@Option(names = "--no-verify", description = "Verify target against source dataset after replication (default: true).", negatable = true)
	private boolean verify = true;

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

	public boolean isVerify() {
		return verify;
	}

	public void setVerify(boolean verify) {
		this.verify = verify;
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
