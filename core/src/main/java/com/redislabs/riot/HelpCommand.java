package com.redislabs.riot;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(usageHelpAutoWidth = true)
public class HelpCommand implements Callable<Integer> {

	@CommandLine.Option(names = "--help", usageHelp = true, description = "Show this help message and exit")
	private boolean helpRequested;

	@Override
	public Integer call() throws Exception {
		CommandLine.usage(this, System.out);
		return 0;
	}

}
