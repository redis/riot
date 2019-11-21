package com.redislabs.riot.cli;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.Riot;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(abbreviateSynopsis = true, usageHelpAutoWidth = true)
public abstract class AbstractCommand implements Runnable {

	@ParentCommand
	private Riot riot;
	@Spec
	private CommandSpec spec;

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit")
	private boolean helpRequested;

	@Override
	public void run() {
		execute(spec.name(), riot.getRedisOptions());
	}

	public abstract void execute(String name, RedisOptions options);

}
