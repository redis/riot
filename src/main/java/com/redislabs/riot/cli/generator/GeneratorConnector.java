package com.redislabs.riot.cli.generator;

import com.redislabs.riot.Riot;
import com.redislabs.riot.cli.HelpAwareCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(name = "gen", description = "Import generated data", subcommands = { FakerGeneratorCommand.class,
		SimpleGeneratorCommand.class })
public class GeneratorConnector extends HelpAwareCommand {

	@ParentCommand
	private Riot riot;

	public Riot riot() {
		return riot;
	}

}
