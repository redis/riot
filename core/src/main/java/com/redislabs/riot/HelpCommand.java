package com.redislabs.riot;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(usageHelpAutoWidth = true)
public class HelpCommand implements Runnable {

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit")
	private boolean helpRequested;

	@Override
	public void run() {
		CommandLine.usage(this, System.out);
	}

}
