package com.redislabs.riot.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(usageHelpAutoWidth = true, showDefaultValues = true, abbreviateSynopsis = true)
public abstract class AbstractCommand implements Runnable {

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit.")
	private boolean help;

	@Override
	public void run() {
		new CommandLine(this).usage(System.out);
	}
}
