package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "db-import", description = "Import database")
public class DatabaseImportCommand extends ImportCommand {

	@ArgGroup(exclusive = false, heading = "Database reader options%n", order = 3)
	private DatabaseReaderOptions options = new DatabaseReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader(RedisOptions redisConnectionOptions) throws Exception {
		return options.reader();
	}

}
