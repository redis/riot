package com.redis.riot;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(usageHelpAutoWidth = true)
public class HelpCommand {

	@Option(names = { "-H", "--help" }, usageHelp = true, description = "Show this help message and exit")
	private boolean helpRequested;

}
