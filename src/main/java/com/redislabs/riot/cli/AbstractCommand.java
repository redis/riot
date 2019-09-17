package com.redislabs.riot.cli;

import com.redislabs.riot.Riot;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractCommand extends HelpAwareCommand implements Runnable {

	@ParentCommand
	private Riot riot;

	protected RedisConnectionOptions getRedisOptions() {
		return riot.getRedisOptions();
	}
}
