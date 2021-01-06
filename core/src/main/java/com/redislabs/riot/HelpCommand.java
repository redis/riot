package com.redislabs.riot;

import picocli.CommandLine;

@CommandLine.Command(usageHelpAutoWidth = true)
public class HelpCommand implements Runnable {

	@CommandLine.Option(names = "--help", usageHelp = true, description = "Show this help message and exit")
	private boolean helpRequested;

	@Override
	public void run() {
		CommandLine.usage(this, System.out);
	}

}
