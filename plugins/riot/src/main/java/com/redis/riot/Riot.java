package com.redis.riot;

import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine.Command;

@Command(name = "riot", versionProvider = Versions.class, subcommands = { DatabaseExport.class, DatabaseImport.class,
		FakerImport.class, FileExport.class, FileImport.class, Generate.class, Ping.class, Replicate.class,
		Compare.class,
		GenerateCompletion.class }, description = "Get data in and out of Redis.", footerHeading = "%nRun 'riot COMMAND --help' for more information on a command.%n%nFor more help on how to use RIOT, head to http://redis.github.io/riot%n")
public class Riot extends RiotMainCommand {

	public static void main(String[] args) {
		System.exit(new Riot().run(args));
	}

}
