package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.picocliredis.RedisOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "redis-import", description = "Import from Redis")
public class RedisImportCommand extends ImportCommand {

	@ArgGroup(exclusive = false, heading = "Source Redis connection options%n")
	private RedisOptions redis = new RedisOptions();
	@ArgGroup(exclusive = false, heading = "Source Redis options%n")
	private RedisReaderOptions options = new RedisReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() throws Exception {
		return options.reader(redis);
	}

}
