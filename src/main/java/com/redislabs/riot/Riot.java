package com.redislabs.riot;

import com.redislabs.riot.cli.ImportCommand;
import org.springframework.batch.item.file.transform.Range;

import com.redislabs.picocliredis.Application;
import com.redislabs.riot.cli.GeneratorCommand;
import com.redislabs.riot.cli.ExportCommand;
import com.redislabs.riot.cli.ReplicateCommand;
import com.redislabs.riot.cli.file.RangeConverter;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riot", subcommands = { ImportCommand.class, ExportCommand.class, GeneratorCommand.class,
		ReplicateCommand.class })
public class Riot extends Application {

	public static void main(String[] args) {
		System.exit(new Riot().execute(args));
	}

	@Override
	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(Range.class, new RangeConverter());
		super.registerConverters(commandLine);
	}

}
