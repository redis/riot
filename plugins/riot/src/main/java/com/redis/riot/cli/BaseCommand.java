package com.redis.riot.cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * @author Julien Ruaux
 */
@Command(usageHelpAutoWidth = true, abbreviateSynopsis = true)
public class BaseCommand {

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit.")
	boolean helpRequested;

	@Mixin
	LoggingMixin loggingMixin = new LoggingMixin();

}
