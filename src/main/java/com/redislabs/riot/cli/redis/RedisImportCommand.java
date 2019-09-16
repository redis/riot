package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "redis-import", description = "Import from Redis")
public class RedisImportCommand extends ImportCommand {

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	private RedisConnectionOptions redis = new RedisConnectionOptions();
	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderOptions redisReader = new RedisReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() {
		return redisReader.reader(redis.jedisPool());
	}

}
