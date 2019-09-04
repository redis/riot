package com.redislabs.riot.cli.test;

import com.redislabs.riot.Riot;
import com.redislabs.riot.cli.HelpAwareCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(name = "test", description = "Connection testing", subcommands = { PingCommand.class, InfoCommand.class })
public class TestConnector extends HelpAwareCommand {

	@ParentCommand
	private Riot riot;

	public Riot riot() {
		return riot;
	}

}
