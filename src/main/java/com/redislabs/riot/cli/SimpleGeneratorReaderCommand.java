package com.redislabs.riot.cli;

import java.util.LinkedHashMap;
import java.util.Map;

import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "simple", description = "Simple generated data")
public class SimpleGeneratorReaderCommand extends AbstractReaderCommand {

	@Option(names = "--field", description = "Field name and size in bytes.", paramLabel = "<name=size>")
	private Map<String, Integer> fields = new LinkedHashMap<>();

	@Override
	public SimpleGeneratorReader reader() {
		SimpleGeneratorReader reader = new SimpleGeneratorReader();
		reader.setFields(fields);
		return reader;
	}

	@Override
	public String getSourceDescription() {
		return "simple-generated";
	}

}
