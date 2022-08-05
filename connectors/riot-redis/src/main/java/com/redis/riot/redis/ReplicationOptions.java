package com.redis.riot.redis;

import com.redis.spring.batch.reader.AbstractKeyspaceNotificationItemReader;

import picocli.CommandLine.Option;

public class ReplicationOptions {

	public enum ReplicationType {
		DUMP, DS
	}

	public enum ReplicationMode {
		SNAPSHOT, LIVE, LIVEONLY
	}

	@Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private ReplicationMode mode = ReplicationMode.SNAPSHOT;
	@Option(names = "--type", description = "Replication type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private ReplicationType type = ReplicationType.DUMP;
	@Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int notificationQueueCapacity = AbstractKeyspaceNotificationItemReader.DEFAULT_QUEUE_CAPACITY;
	@Option(names = "--no-verify", description = "Verify target against source dataset after replication. True by default", negatable = true)
	private boolean verify = true;
	@Option(names = "--dry-run", description = "Disable writes and only perform reads")
	private boolean dryRun;

	public ReplicationMode getMode() {
		return mode;
	}

	public void setMode(ReplicationMode mode) {
		this.mode = mode;
	}

	public ReplicationType getType() {
		return type;
	}

	public void setType(ReplicationType type) {
		this.type = type;
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

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

}
