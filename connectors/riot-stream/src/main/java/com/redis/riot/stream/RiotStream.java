package com.redis.riot.stream;

import com.redis.riot.RiotApp;

import picocli.CommandLine.Command;

@Command(name = "riot-stream", subcommands = { StreamImportCommand.class, StreamExportCommand.class })
public class RiotStream extends RiotApp {

	public static void main(String[] args) {
		System.exit(new RiotStream().execute(args));
	}
}
