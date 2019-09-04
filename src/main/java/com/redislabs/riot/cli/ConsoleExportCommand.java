package com.redislabs.riot.cli;

import com.redislabs.riot.Riot;
import com.redislabs.riot.batch.ConsoleWriter;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(name = "console", description = "Export to console")
public class ConsoleExportCommand extends ExportCommand {

	@ParentCommand
	private Riot riot;

	public ConsoleWriter writer() {
		return new ConsoleWriter();
	}

	@Override
	protected String name() {
		return "console-export";
	}

	@Override
	protected RedisConnectionOptions redis() {
		return riot.redis();
	}

}
