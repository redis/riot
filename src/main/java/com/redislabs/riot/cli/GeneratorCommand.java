package com.redislabs.riot.cli;

import com.redislabs.riot.cli.gen.GeneratorOptions;
import com.redislabs.riot.generator.GeneratorReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "gen", description = "Generate random data in Redis")
public class GeneratorCommand extends ImportCommand {

	@ArgGroup(exclusive = false, heading = "Generator options%n")
	private GeneratorOptions gen = new GeneratorOptions();

	@Override
	protected GeneratorReader reader() throws Exception {
		return gen.reader(redisOptions());
	}

	@Override
	protected String taskName() {
		return "Generating";
	}

}
