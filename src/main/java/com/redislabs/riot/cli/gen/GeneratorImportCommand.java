package com.redislabs.riot.cli.gen;

import com.redislabs.riot.batch.generator.GeneratorReader;
import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "gen", description = "Generate data")
public class GeneratorImportCommand extends ImportCommand {

	@ArgGroup(exclusive = false, heading = "Generator options%n", order = 2)
	private GeneratorOptions options = new GeneratorOptions();

	@Override
	protected GeneratorReader reader() throws Exception {
		return options.reader();
	}

}
