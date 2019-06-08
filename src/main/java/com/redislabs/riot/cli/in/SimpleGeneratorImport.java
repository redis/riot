package com.redislabs.riot.cli.in;

import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;

@Command(name = "simple", description = "Import simple generated data")
public class SimpleGeneratorImport extends AbstractImportReaderCommand {

	@Override
	public SimpleGeneratorReader reader() {
		return new SimpleGeneratorReader();
	}

	@Override
	public String getSourceDescription() {
		return "simple-generated";
	}

}
