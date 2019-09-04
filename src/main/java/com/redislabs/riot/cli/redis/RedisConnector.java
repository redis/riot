package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.Riot;
import com.redislabs.riot.cli.ExportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(name = "redis", description = "Redis to Redis")
public class RedisConnector extends ExportCommand {

	@ParentCommand
	private Riot riot;

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	private RedisConnectionOptions redis = new RedisConnectionOptions();
	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterOptions writer = new RedisWriterOptions();

	@Override
	protected ItemWriter<Map<String, Object>> writer() throws Exception {
		return writer.writer(redis);
	}

	@Override
	protected String name() {
		return "redis-export";
	}

	@Override
	protected RedisConnectionOptions redis() {
		return riot.redis();
	}

}
