package com.redislabs.riot;

import org.springframework.batch.item.file.transform.Range;

import com.redislabs.picocliredis.Main;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.cli.RedisImportCommand;
import com.redislabs.riot.cli.TestCommand;
import com.redislabs.riot.cli.db.DatabaseExportCommand;
import com.redislabs.riot.cli.db.DatabaseImportCommand;
import com.redislabs.riot.cli.file.FileExportCommand;
import com.redislabs.riot.cli.file.FileImportCommand;
import com.redislabs.riot.cli.file.RangeConverter;
import com.redislabs.riot.cli.gen.GeneratorImportCommand;

import lombok.Getter;
import lombok.experimental.Accessors;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Accessors(fluent = true)
@Command(name = "riot", subcommands = { FileImportCommand.class, FileExportCommand.class, DatabaseImportCommand.class,
		DatabaseExportCommand.class, RedisImportCommand.class, GeneratorImportCommand.class, TestCommand.class })
public class Riot extends Main {

	@Getter
	@ArgGroup(exclusive = false, heading = "Redis connection options%n")
	private RedisOptions redisOptions = new RedisOptions();

	public static void main(String[] args) {
		System.exit(new Riot().execute(args));
	}

	@Override
	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(Range.class, new RangeConverter());
		super.registerConverters(commandLine);
	}

}
