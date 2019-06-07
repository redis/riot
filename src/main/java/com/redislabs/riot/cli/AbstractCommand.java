package com.redislabs.riot.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(mixinStandardHelpOptions = true)
public abstract class AbstractCommand implements Runnable {

	@Override
	public void run() {
		new CommandLine(this).usage(System.out);
	}
}
