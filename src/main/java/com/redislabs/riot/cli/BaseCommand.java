package com.redislabs.riot.cli;

import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class BaseCommand implements Runnable {

	@Getter
	@Option(names = { "--help" }, usageHelp = true, description = "Prints this help message and exits")
	private boolean helpRequested;

	@Override
	public void run() {
		CommandLine.usage(this, System.out);
	}

}
