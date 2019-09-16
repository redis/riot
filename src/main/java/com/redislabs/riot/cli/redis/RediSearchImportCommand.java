package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "redisearch-import", description = "Import from RediSearch")
public class RediSearchImportCommand extends ImportCommand {

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	private RedisConnectionOptions redis = new RedisConnectionOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch reader options%n")
	private RediSearchReaderOptions searchReader = new RediSearchReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() {
		return searchReader.reader(redis.rediSearchClient());
	}

}
