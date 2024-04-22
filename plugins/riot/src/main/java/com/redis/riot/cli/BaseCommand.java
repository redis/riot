package com.redis.riot.cli;

import java.util.Map;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(usageHelpAutoWidth = true, abbreviateSynopsis = true)
abstract class BaseCommand {

	static {
		if (System.getenv().containsKey("RIOT_NO_COLOR")) {
			System.setProperty("picocli.ansi", "false");
		}
	}

	@Spec
	CommandSpec spec;

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit.")
	boolean helpRequested;

	@CommandLine.Option(names = "-D", paramLabel = "<key=value>", descriptionKey = "system-property", mapFallbackValue = "", hidden = true)
	void setProperty(Map<String, String> props) {
		props.forEach(System::setProperty);
	}

}