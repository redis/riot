package com.redis.riot.cli.common;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.redis.riot.cli.operation.OperationCommand;

import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

public class RiotExecutionStrategy implements IExecutionStrategy {

	private final List<IExecutionStrategy> strategies;

	public RiotExecutionStrategy(IExecutionStrategy... strategies) {
		this.strategies = Arrays.asList(strategies);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int execute(ParseResult parseResult) {
		for (IExecutionStrategy strategy : strategies) {
			strategy.execute(parseResult);
		}
		for (ParseResult subcommand : parseResult.subcommands()) {
			Object command = subcommand.commandSpec().userObject();
			if (AbstractOperationImportCommand.class.isAssignableFrom(command.getClass())) {
				AbstractOperationImportCommand importCommand = (AbstractOperationImportCommand) command;
				for (ParseResult redisCommand : subcommand.subcommands()) {
					if (redisCommand.isUsageHelpRequested()) {
						return new RunLast().execute(redisCommand);
					}
					importCommand.getRedisCommands()
							.add((OperationCommand<Map<String, Object>>) redisCommand.commandSpec().userObject());
				}
				return new RunFirst().execute(subcommand);
			}
		}
		return new RunLast().execute(parseResult); // default execution strategy
	}
}