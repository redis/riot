package com.redislabs.riot.cli;

import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.Riot;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public @Data class RiotCommand extends HelpCommand {

	@ParentCommand
	@Getter(AccessLevel.NONE)
	@Setter(AccessLevel.NONE)
	private Riot parent;

	public RedisOptions redisOptions() {
		return parent.redisOptions();
	}

	protected Riot parent() {
		return parent;
	}

}
