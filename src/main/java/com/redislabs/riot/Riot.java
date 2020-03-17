package com.redislabs.riot;

import org.springframework.batch.item.file.transform.Range;

import com.redislabs.picocliredis.Main;
import com.redislabs.riot.cli.GeneratorCommand;
import com.redislabs.riot.cli.MapExportCommand;
import com.redislabs.riot.cli.MapImportCommand;
import com.redislabs.riot.cli.ReplicateCommand;
import com.redislabs.riot.cli.file.RangeConverter;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riot", subcommands = { MapImportCommand.class, MapExportCommand.class, GeneratorCommand.class,
		ReplicateCommand.class })
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
