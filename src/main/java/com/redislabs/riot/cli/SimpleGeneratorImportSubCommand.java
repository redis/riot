package com.redislabs.riot.cli;

import org.springframework.stereotype.Component;

import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;

@Component
@Command(name = "sgen", description = "Import simple generated data")
public class SimpleGeneratorImportSubCommand extends ImportSubCommand {

	@Override
	public SimpleGeneratorReader reader() {
		SimpleGeneratorReader reader = new SimpleGeneratorReader();
		reader.setClient(getParent().getParent().redisConnectionBuilder().buildLettuceClient());
		return reader;
	}

	public String getSourceDescription() {
		return "simple-generated";
	}

}
