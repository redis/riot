package com.redislabs.riot.cli;

import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.Riot;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public class RiotCommand extends HelpCommand {

	@ParentCommand
	private Riot parent;

	public RedisOptions redisOptions() {
		return parent.getRedisOptions();
	}

	protected Riot parent() {
		return parent;
	}
}
