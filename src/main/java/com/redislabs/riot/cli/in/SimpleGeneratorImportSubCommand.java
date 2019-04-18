package com.redislabs.riot.cli.in;

import org.springframework.stereotype.Component;

import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;

@Component
@Command(name = "sgen", description = "Import simple generated data")
public class SimpleGeneratorImportSubCommand extends AbstractImportSubCommand {

	@Override
	public SimpleGeneratorReader reader() {
		SimpleGeneratorReader reader = new SimpleGeneratorReader();
		reader.setClient(getParent().redisConnectionBuilder().buildLettuceClient());
		return reader;
	}

	@Override
	public String getSourceDescription() {
		return "simple-generated";
	}

}
