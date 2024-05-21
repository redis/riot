package com.redis.riot.cli;

import picocli.CommandLine.Command;

@Command(name = "riot", mixinStandardHelpOptions = true, versionProvider = Versions.class, headerHeading = "RIOT is a data import/export tool for Redis.%n%n", footerHeading = "%nDocumentation found at http://redis.github.io/riot%n")
public class Main extends AbstractMainCommand {

	public static void main(String[] args) {
		System.exit(run(new Main(), args));
	}

}
