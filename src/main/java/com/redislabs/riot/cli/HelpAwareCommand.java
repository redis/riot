package com.redislabs.riot.cli;

import java.util.concurrent.Callable;

import picocli.CommandLine.Option;

public class HelpAwareCommand implements Callable<Void> {

	@Option(names = { "--help" }, usageHelp = true, order = 0)
	private boolean helpRequested;

	@Override
	public Void call() throws Exception {
		return null;
	}

}
