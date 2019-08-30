package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.db.DatabaseExportCommand;
import com.redislabs.riot.cli.file.FileExportCommand;
import com.redislabs.riot.cli.redis.RediSearchReaderOptions;
import com.redislabs.riot.cli.redis.RedisExportCommand;
import com.redislabs.riot.cli.redis.RedisReaderOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "export", description = "Export data from Redis", subcommands = { FileExportCommand.class,
		DatabaseExportCommand.class, RedisExportCommand.class, ConsoleExportCommand.class })
public class ExportParentCommand extends TransferCommand {

	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderOptions redisReader = new RedisReaderOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch reader options%n")
	private RediSearchReaderOptions searchReader = new RediSearchReaderOptions();

	public void transfer(ItemWriter<Map<String, Object>> writer) {
		transfer("export", reader(), writer);
	}

	private ItemReader<Map<String, Object>> reader() {
		if (searchReader.isSet()) {
			return searchReader.reader(getRiot().getRedis().rediSearchClient());
		}
		return redisReader.reader(getRiot().getRedis().jedisPool());
	}

}
