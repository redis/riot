package com.redislabs.riot.cli;

import com.redislabs.picocliredis.RedisOptions;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class ReplicateOptions {

	private final static String DATABASE_TOKEN = "{database}";

	@Option(names = "--notification-queue", description = "Capacity of the keyspace notification queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int notificationQueue = 10000;
	@Option(names = "--keyspace-channel", description = "Pub/sub channel for keyspace events (default: ${DEFAULT-VALUE}). Blank to disable", paramLabel = "<string>")
	private String channel = "__keyspace@" + DATABASE_TOKEN + "__:*";
	@Option(names = "--no-replace", description = "No REPLACE modifier with RESTORE command")
	private boolean noReplace;
	@Option(names = "--flush-rate", description = "Interval in millis between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long flushRate = 50;
	@Option(names = "--syncer-timeout", description = "Syncer timeout duration in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private int timeout = 2;
	@Option(names = "--syncer-pipeline", description = "Number of values in dump pipeline (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int pipeline = 50;
	@Option(names = "--syncer-queue", description = "Capacity of the value queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int queueSize = 10000;
	@Option(names = "--syncer-threads", description = "Number of value reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@Option(names = "--listener", description = "Enable keyspace notification listener")
	private boolean listener;

	public String getChannel(RedisOptions redisOptions) {
		return channel.replace(DATABASE_TOKEN, String.valueOf(redisOptions.getDatabase()));
	}

}
