package com.redislabs.riot.cli;

import java.text.MessageFormat;
import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.db.DatabaseExportCommand;
import com.redislabs.riot.cli.file.FileExportCommand;
import com.redislabs.riot.cli.redis.RediSearchReaderOptions;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;
import com.redislabs.riot.cli.redis.RedisExportCommand;
import com.redislabs.riot.cli.redis.RedisReaderOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "export", description = "Export data from Redis", subcommands = { FileExportCommand.class,
		DatabaseExportCommand.class, RedisExportCommand.class })
public class ExportParentCommand extends TransferCommand {

	@ArgGroup(exclusive = false, heading = "Redis connection options%n")
	private RedisConnectionOptions redis = new RedisConnectionOptions();
	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderOptions redisReader = new RedisReaderOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch reader options%n")
	private RediSearchReaderOptions searchReader = new RediSearchReaderOptions();

	public void transfer(ItemWriter<Map<String, Object>> writer) {
		transfer(reader(), writer);
	}

	private ItemReader<Map<String, Object>> reader() {
		if (searchReader.isSet()) {
			return searchReader.reader(redis.clientResources(), redis.redisUri());
		}
		return redisReader.reader(redis.jedis());
	}

	public String sourceDescription() {
		if (searchReader.isSet()) {
			return MessageFormat.format("RediSearch index %s query %s", searchReader.getIndex(),
					searchReader.getQuery());
		}
		return MessageFormat.format("Redis %s", redisReader.scanPattern());
	}
}
