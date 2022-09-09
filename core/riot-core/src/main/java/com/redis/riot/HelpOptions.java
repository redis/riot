package com.redis.riot;

import picocli.CommandLine.Option;

public final class HelpOptions {

	@Option(names = { "-H", "--help" }, usageHelp = true, description = "Show this help message and exit.")
	private boolean helpRequested;

}
