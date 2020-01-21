package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.cli.MapImportCommand;

import lombok.Data;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "redis-import", description = "Import from Redis")
public @Data class RedisImportCommand extends MapImportCommand {

	@ArgGroup(exclusive = false, heading = "Source Redis connection options%n")
	private RedisOptions redis = new RedisOptions();
	@ArgGroup(exclusive = false, heading = "Source Redis options%n")
	private HashReaderOptions options = new HashReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() {
		return options.reader(redis);
	}

}
