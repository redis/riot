package com.redis.riot;

import java.util.List;
import java.util.Map;

import picocli.CommandLine;

public class RiotCommandLine extends CommandLine {

	private final IExecutionStrategy executionStrategy;

	public RiotCommandLine(Main app, IExecutionStrategy executionStrategy) {
		super(app);
		this.executionStrategy = executionStrategy;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ParseResult parseArgs(String... args) {
		ParseResult parseResult = super.parseArgs(args);
		ParseResult subcommand = parseResult.subcommand();
		if (subcommand != null) {
			Object command = subcommand.commandSpec().userObject();
			if (AbstractImportCommand.class.isAssignableFrom(command.getClass())) {
				AbstractImportCommand importCommand = (AbstractImportCommand) command;
				List<ParseResult> parsedRedisCommands = subcommand.subcommands();
				for (ParseResult parsedRedisCommand : parsedRedisCommands) {
					if (parsedRedisCommand.isUsageHelpRequested()) {
						return parsedRedisCommand;
					}
					importCommand.getRedisCommands()
							.add((OperationCommand<Map<String, Object>>) parsedRedisCommand.commandSpec().userObject());
				}
				setExecutionStrategy(executionStrategy);
				return subcommand;
			}
		}
		return parseResult;
	}
}