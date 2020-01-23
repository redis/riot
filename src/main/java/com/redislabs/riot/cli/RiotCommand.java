package com.redislabs.riot.cli;

import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.Riot;

import lombok.Data;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public @Data class RiotCommand extends HelpCommand {

	@ParentCommand
	private Riot parent = new Riot();

	public RedisOptions redisOptions() {
		return parent.redisOptions();
	}

	protected Riot parent() {
		return parent;
	}

}
