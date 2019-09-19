package com.redislabs.riot.cli.generator;

import java.util.LinkedHashMap;
import java.util.Map;

import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.generator.GeneratorReader;
import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "gen", description = "Import simple generated data")
public class SimpleGeneratorCommand extends ImportCommand {

	@Option(names = "--fields", arity = "0..*", description = "Field sizes in bytes", paramLabel = "<field=size>")
	private Map<String, Integer> fields = new LinkedHashMap<>();

	@Override
	protected GeneratorReader reader() {
		return new SimpleGeneratorReader(fields);
	}

}
