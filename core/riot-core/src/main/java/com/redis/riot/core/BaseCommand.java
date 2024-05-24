package com.redis.riot.core;

import java.util.Map;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(usageHelpAutoWidth = true)
abstract class BaseCommand {

	static {
		if (System.getenv().containsKey("RIOT_NO_COLOR")) {
			System.setProperty("picocli.ansi", "false");
		}
	}

	@Spec
	CommandSpec commandSpec;

	@Option(names = "-D", paramLabel = "<key=value>", description = "Sets a System property.", mapFallbackValue = "")
	void setProperty(Map<String, String> props) {
		props.forEach(System::setProperty);
	}

}
