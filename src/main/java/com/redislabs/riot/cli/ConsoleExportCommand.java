package com.redislabs.riot.cli;

import com.redislabs.riot.batch.ConsoleWriter;

import picocli.CommandLine.Command;

@Command(name = "console", description = "Redis -> console")
public class ConsoleExportCommand {

	public ConsoleWriter writer() {
		return new ConsoleWriter();
	}

}
