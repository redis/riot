package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.TransferContext;
import com.redislabs.riot.cli.MapImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "redis-import", description = "Import from Redis")
public class RedisImportCommand extends MapImportCommand {

	@ArgGroup(exclusive = false, heading = "Source Redis connection options%n")
	private RedisOptions redis = new RedisOptions();
	@ArgGroup(exclusive = false, heading = "Source Redis options%n")
	private RedisHashReaderOptions options = new RedisHashReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader(TransferContext context) {
		return options.reader(redis);
	}

}
