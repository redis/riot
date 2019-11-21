package com.redislabs.riot;

import org.springframework.batch.item.file.transform.Range;

import com.redislabs.picocliredis.Main;
import com.redislabs.riot.cli.ConsoleExportCommand;
import com.redislabs.riot.cli.GeneratorCommand;
import com.redislabs.riot.cli.db.DatabaseExportCommand;
import com.redislabs.riot.cli.db.DatabaseImportCommand;
import com.redislabs.riot.cli.file.FileExportCommand;
import com.redislabs.riot.cli.file.FileImportCommand;
import com.redislabs.riot.cli.file.RangeConverter;
import com.redislabs.riot.cli.redis.RedisExportCommand;
import com.redislabs.riot.cli.test.TestCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riot", subcommands = { FileImportCommand.class, FileExportCommand.class, DatabaseImportCommand.class,
		DatabaseExportCommand.class, RedisExportCommand.class, ConsoleExportCommand.class, GeneratorCommand.class,
		TestCommand.class })
public class Riot extends Main {

	public static void main(String[] args) {
		System.exit(new Riot().execute(args));
	}

	@Override
	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(Range.class, new RangeConverter());
		super.registerConverters(commandLine);
	}

}
