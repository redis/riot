package com.redislabs.riot.cli;

import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;

@Command(name = "sgen", description = "Import simple generated data")
public class SimpleGeneratorImport extends ImportSub {

	@Override
	public SimpleGeneratorReader reader() {
		return new SimpleGeneratorReader();
	}

	@Override
	public String getSourceDescription() {
		return "simple-generated";
	}

}
