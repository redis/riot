package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.ExportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "redis", description = "Export to another Redis database")
public class RedisExportCommand extends ExportCommand {

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

}
