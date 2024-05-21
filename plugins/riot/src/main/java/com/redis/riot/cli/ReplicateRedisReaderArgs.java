package com.redis.riot.cli;

import com.redis.riot.redis.Replication;

import picocli.CommandLine.Option;

public class ReplicateRedisReaderArgs extends RedisReaderArgs {

	@Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int notificationQueueCapacity = Replication.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
	}

}
