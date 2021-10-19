package com.redis.riot.redis;

import com.redis.spring.batch.support.LiveRedisItemReaderBuilder;

import lombok.Data;
import picocli.CommandLine;

@Data
public class ReplicationOptions {

    @CommandLine.Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private ReplicationMode mode = ReplicationMode.SNAPSHOT;
    @CommandLine.Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int notificationQueueCapacity = LiveRedisItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
    @CommandLine.Option(names = "--no-verify", description = "Verify target against source dataset after replication. True by default", negatable = true)
    private boolean verify = true;
}
