package com.redislabs.riot.cli;

import com.redislabs.riot.Riot;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(abbreviateSynopsis = true, usageHelpAutoWidth = true)
public abstract class AbstractCommand implements Runnable {

	@ParentCommand
	private Riot riot;

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit")
	private boolean helpRequested;

	protected RedisConnectionOptions getRedisOptions() {
		return riot.getRedisOptions();
	}

}
