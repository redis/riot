package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.ExportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "redis", description = "Redis -> Redis")
public class RedisExportCommand extends ExportCommand {

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	private RedisConnectionOptions redis = new RedisConnectionOptions();
	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterOptions redisWriter = new RedisWriterOptions();

	@Override
	protected ItemWriter<Map<String, Object>> writer() throws Exception {
		return redisWriter.writer(parent.getRiot().getRedis());
	}

}
