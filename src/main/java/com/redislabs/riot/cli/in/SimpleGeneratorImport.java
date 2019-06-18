package com.redislabs.riot.cli.in;

import java.util.LinkedHashMap;
import java.util.Map;

import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "simple", description = "Generate random fixed-size string fields")
public class SimpleGeneratorImport extends AbstractImportReaderCommand {

	@Option(names = "--field", description = "Field name and size in bytes.", paramLabel = "<name=size>")
	private Map<String, Integer> fields = new LinkedHashMap<>();

	@Override
	public SimpleGeneratorReader reader() {
		return new SimpleGeneratorReader(fields);
	}

	@Override
	public String getSourceDescription() {
		return "simple-generated";
	}

}
