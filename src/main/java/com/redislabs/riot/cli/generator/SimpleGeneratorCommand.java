package com.redislabs.riot.cli.generator;

import java.util.Map;

import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;
import com.redislabs.riot.generator.GeneratorReader;
import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Command(name = "simple", description = "Generate simple data")
public class SimpleGeneratorCommand extends ImportCommand {

	@ParentCommand
	private GeneratorConnector connector;

	@Parameters(description = "Field sizes in bytes", paramLabel = "<field=size>")
	private Map<String, Integer> fields;

	@Override
	protected GeneratorReader reader() {
		return new SimpleGeneratorReader(fields);
	}

	@Override
	protected RedisConnectionOptions redis() {
		return connector.riot().redis();
	}

	@Override
	protected String name() {
		return "simple-gen-import";
	}

}
