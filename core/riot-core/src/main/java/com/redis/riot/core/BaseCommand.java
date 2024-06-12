package com.redis.riot.core;

import java.util.Map;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(usageHelpAutoWidth = true, mixinStandardHelpOptions = true, abbreviateSynopsis = true)
public abstract class BaseCommand {

	static {
		if (System.getenv().containsKey("RIOT_NO_COLOR")) {
			System.setProperty("picocli.ansi", "false");
		}
	}

	@Spec
	protected CommandSpec commandSpec;

	@Option(names = "-D", paramLabel = "<key=value>", description = "Sets a System property.", mapFallbackValue = "", hidden = true)
	void setProperty(Map<String, String> props) {
		props.forEach(System::setProperty);
	}

}
