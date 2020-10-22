package com.redislabs.riot.redis;

import com.redislabs.riot.HelpCommand;
import com.redislabs.riot.RiotApp;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public abstract class AbstractRedisCommand extends HelpCommand {

	@ParentCommand
	private RiotApp app;

	@Override
	public void run() {
		StatefulConnection<String, String> connection = app.connection();
		BaseRedisCommands<String, String> commands = app.sync(connection);
		execute(commands);
	}

	protected abstract void execute(BaseRedisCommands<String, String> commands);

}
