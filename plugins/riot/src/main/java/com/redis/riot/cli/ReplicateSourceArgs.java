package com.redis.riot.cli;

import com.redis.riot.redis.Replication;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ReplicateSourceArgs {

	@ArgGroup(exclusive = false)
	private RedisClientArgs redisClientArgs = new RedisClientArgs();

	@ArgGroup(exclusive = false)
	private RedisReaderArgs redisReaderArgs = new RedisReaderArgs();

	@Option(names = "--event-queue", description = "Capacity of the keyspace notification event queue (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int notificationQueueCapacity = Replication.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	public RedisClientArgs getRedisClientArgs() {
		return redisClientArgs;
	}

	public void setRedisClientArgs(RedisClientArgs args) {
		this.redisClientArgs = args;
	}

	public RedisReaderArgs getRedisReaderArgs() {
		return redisReaderArgs;
	}

	public void setRedisReaderArgs(RedisReaderArgs args) {
		this.redisReaderArgs = args;
	}

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
	}

}
