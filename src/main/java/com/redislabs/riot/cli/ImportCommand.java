package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.redis.RediSearchWriterOptions;
import com.redislabs.riot.cli.redis.RedisConnectionPoolOptions;
import com.redislabs.riot.cli.redis.RedisWriterOptions;

import picocli.CommandLine.ArgGroup;

public abstract class ImportCommand extends TransferCommand {

	@ArgGroup(exclusive = false, heading = "Redis connection options%n")
	private RedisConnectionPoolOptions redis = new RedisConnectionPoolOptions();
	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterOptions redisWriter = new RedisWriterOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch writer options%n")
	private RediSearchWriterOptions searchWriter = new RediSearchWriterOptions();

	@Override
	protected ItemWriter<Map<String, Object>> writer() throws Exception {
		if (searchWriter.isSet()) {
			return redis.writer(searchWriter.writer());
		}
		return redis.writer(redisWriter.writer());
	}

	@Override
	protected String targetDescription() {
		if (searchWriter.isSet()) {
			return String.format("RediSearch index %s", searchWriter.getIndex());
		}
		return String.format("Redis %s %s", redisWriter.getCommand(), redisWriter.keyspaceDescription());
	}

}
