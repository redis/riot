package com.redislabs.riot.cli;

import picocli.CommandLine.Option;

public class HelpAwareCommand extends BaseCommand {

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit")
	private boolean help;

}
