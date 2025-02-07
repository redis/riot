package com.redis.riot;

import com.redis.riot.core.MainCommand;
import com.redis.riot.operation.OperationCommand;
import com.redis.spring.batch.item.redis.common.Range;

import io.lettuce.core.RedisURI;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;
import software.amazon.awssdk.regions.Region;

public class RiotMainCommand extends MainCommand {

	@Override
	protected int executionStrategy(ParseResult parseResult) {
		for (ParseResult subcommand : parseResult.subcommands()) {
			Object command = subcommand.commandSpec().userObject();
			if (AbstractImportCommand.class.isAssignableFrom(command.getClass())) {
				AbstractImportCommand importCommand = (AbstractImportCommand) command;
				for (ParseResult redisCommand : subcommand.subcommands()) {
					if (redisCommand.isUsageHelpRequested()) {
						return new RunLast().execute(redisCommand);
					}
					OperationCommand operationCommand = (OperationCommand) redisCommand.commandSpec().userObject();
					importCommand.getImportOperationCommands().add(operationCommand);
				}
				return new RunFirst().execute(subcommand);
			}
		}
		return new RunLast().execute(parseResult); // default execution strategy
	}

	@Override
	protected void registerConverters(CommandLine commandLine) {
		super.registerConverters(commandLine);
		commandLine.registerConverter(RedisURI.class, new RedisURIConverter());
		commandLine.registerConverter(Region.class, Region::of);
		commandLine.registerConverter(Range.class, new RangeConverter());
	}

}
