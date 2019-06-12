package com.redislabs.riot.cli.in;

import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "simple", description = "Import simple generated data")
public class SimpleGeneratorImport extends AbstractImportReaderCommand {

	@Option(names = "--fields", description = "Number of fields to generate. (default: ${DEFAULT-VALUE}).")
	private int fieldCount = 3;

	@Override
	public SimpleGeneratorReader reader() {
		return new SimpleGeneratorReader(fieldCount);
	}

	@Override
	public String getSourceDescription() {
		return "simple-generated";
	}

}
