package com.redis.riot;

import com.redis.riot.operation.OperationCommand;

import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

public class RiotExecutionStrategy implements IExecutionStrategy {

	private IExecutionStrategy defaultStrategy = new RunLast();

	public void setDefaultStrategy(IExecutionStrategy defaultStrategy) {
		this.defaultStrategy = defaultStrategy;
	}

	@Override
	public int execute(ParseResult parseResult) throws ExecutionException, ParameterException {
		for (ParseResult subcommand : parseResult.subcommands()) {
			Object command = subcommand.commandSpec().userObject();
			if (AbstractImportCommand.class.isAssignableFrom(command.getClass())) {
				AbstractImportCommand importCommand = (AbstractImportCommand) command;
				for (ParseResult redisCommand : subcommand.subcommands()) {
					if (redisCommand.isUsageHelpRequested()) {
						return new RunLast().execute(redisCommand);
					}
					importCommand.getImportOperationCommands()
							.add((OperationCommand) redisCommand.commandSpec().userObject());
				}
				return new RunFirst().execute(subcommand);
			}
		}
		return defaultStrategy.execute(parseResult); // default execution strategy
	}

}
