package com.redislabs.riot.cli.generator;

import java.util.Map;

import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.generator.GeneratorReader;
import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "simple", description = "Simple generator -> Redis")
public class SimpleGeneratorCommand extends ImportCommand {

	@Parameters(description = "Field sizes in bytes", paramLabel = "<field=size>")
	private Map<String, Integer> fields;

	@Override
	protected GeneratorReader reader() {
		return new SimpleGeneratorReader(fields);
	}

}
