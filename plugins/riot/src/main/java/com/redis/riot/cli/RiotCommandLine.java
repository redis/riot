package com.redis.riot.cli;

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
				for (ParseResult redisCommand : parsedRedisCommands) {
					if (redisCommand.isUsageHelpRequested()) {
						return redisCommand;
					}
					OperationCommand<Map<String, Object>> opCommand = (OperationCommand<Map<String, Object>>) redisCommand
							.commandSpec().userObject();
					importCommand.getRedisCommands().add(opCommand);
				}
				setExecutionStrategy(executionStrategy);
				return subcommand;
			}
		}
		return parseResult;
	}
}