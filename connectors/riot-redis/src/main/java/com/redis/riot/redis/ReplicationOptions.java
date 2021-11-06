package com.redis.riot.redis;

import com.redis.spring.batch.support.LiveKeyItemReader;

import lombok.Data;
import picocli.CommandLine;

@Data
public class ReplicationOptions {

	public enum ReplicationType {
		DUMP, DS
	}

	public enum ReplicationMode {
		SNAPSHOT, LIVE, LIVEONLY
	}

	@CommandLine.Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private ReplicationMode mode = ReplicationMode.SNAPSHOT;
	@CommandLine.Option(names = "--type", description = "Replication type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private ReplicationType type = ReplicationType.DUMP;
	@CommandLine.Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int notificationQueueCapacity = LiveKeyItemReader.DEFAULT_QUEUE_CAPACITY;
	@CommandLine.Option(names = "--no-verify", description = "Verify target against source dataset after replication. True by default", negatable = true)
	private boolean verify = true;
}
