package com.redislabs.riot.stream;

import com.redislabs.riot.RiotApp;

import picocli.CommandLine;

@CommandLine.Command(name = "riot-stream", subcommands = { StreamImportCommand.class, StreamExportCommand.class })
public class RiotStream extends RiotApp {

	public static void main(String[] args) {
		System.exit(new RiotStream().execute(args));
	}
}
