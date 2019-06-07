package com.redislabs.riot;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(mixinStandardHelpOptions = true)
public class BaseCommand implements Runnable {

	@Override
	public void run() { 
		new CommandLine(this).usage(System.out);
	}
}
