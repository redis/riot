package com.redis.riot;

import java.util.ArrayList;
import java.util.List;

import com.redis.riot.core.AbstractMain;
import com.redis.riot.operation.OperationCommand;

import io.lettuce.core.RedisURI;
import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

@Command(name = "riot", versionProvider = Versions.class, headerHeading = "RIOT is a data import/export tool for Redis.%n%n", footerHeading = "%nDocumentation found at http://redis.github.io/riot%n", subcommands = {
		DatabaseImport.class, DatabaseExport.class, FileImport.class, FileExport.class, FakerImport.class,
		Generate.class, Replicate.class, Compare.class, Ping.class, GenerateCompletion.class })
public class Main extends AbstractMain {

	public static void main(String[] args) {
		System.exit(run(new Main(), args));
	}

	@Override
	protected CommandLine commandLine() {
		CommandLine commandLine = super.commandLine();
		commandLine.registerConverter(RedisURI.class, RedisURI::create);
		return commandLine;
	}

	@Override
	protected List<IExecutionStrategy> executionStrategies() {
		List<IExecutionStrategy> strategies = new ArrayList<>();
		strategies.addAll(super.executionStrategies());
		strategies.add(Main::executionStrategy);
		return strategies;
	}

	private static int executionStrategy(ParseResult parseResult) {
		for (ParseResult subcommand : parseResult.subcommands()) {
			Object command = subcommand.commandSpec().userObject();
			if (AbstractImport.class.isAssignableFrom(command.getClass())) {
				AbstractImport importCommand = (AbstractImport) command;
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
		return new RunLast().execute(parseResult); // default execution strategy
	}
}
