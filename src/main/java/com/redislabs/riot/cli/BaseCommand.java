package com.redislabs.riot.cli;

import picocli.CommandLine;

public class BaseCommand implements Runnable {

	@Override
	public void run() {
		CommandLine.usage(this, System.out);
	}
}
