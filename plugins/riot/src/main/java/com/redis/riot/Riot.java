package com.redis.riot;

import java.util.Arrays;
import java.util.List;

import com.redis.riot.core.MainCommand;
import com.redis.riot.operation.OperationCommand;
import com.redis.spring.batch.item.redis.common.Range;

import io.lettuce.core.RedisURI;
import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import software.amazon.awssdk.regions.Region;

@Command(name = "riot", versionProvider = Versions.class, headerHeading = "A data import/export tool for Redis.%n%n", footerHeading = "%nRun 'riot COMMAND --help' for more information on a command.%n%nFor more help on how to use RIOT, head to http://redis.github.io/riot%n")
public class Riot extends MainCommand {

	public static void main(String[] args) {
		System.exit(new Riot().run(args));
	}

	@Override
	protected CommandLine commandLine() {
		CommandLine commandLine = super.commandLine();
		subcommands().forEach(commandLine::addSubcommand);
		IExecutionStrategy defaultStrategy = commandLine.getExecutionStrategy();
		commandLine.setExecutionStrategy(r -> execute(r, defaultStrategy));
		commandLine.registerConverter(RedisURI.class, new RedisURIConverter());
		commandLine.registerConverter(Region.class, Region::of);
		commandLine.registerConverter(Range.class, new RangeConverter());
		return commandLine;
	}

	protected List<Object> subcommands() {
		return Arrays.asList(databaseExport(), databaseImport(), fakerImport(), fileExport(), fileImport(), generate(),
				ping(), replicate(), compare(), generateCompletion());
	}

	protected GenerateCompletion generateCompletion() {
		return new GenerateCompletion();
	}

	protected Compare compare() {
		return new Compare();
	}

	protected Replicate replicate() {
		return new Replicate();
	}

	protected Ping ping() {
		return new Ping();
	}

	protected Generate generate() {
		return new Generate();
	}

	protected FileImport fileImport() {
		return new FileImport();
	}

	protected FileExport fileExport() {
		return new FileExport();
	}

	protected FakerImport fakerImport() {
		return new FakerImport();
	}

	protected DatabaseImport databaseImport() {
		return new DatabaseImport();
	}

	protected DatabaseExport databaseExport() {
		return new DatabaseExport();
	}

	protected int execute(ParseResult parseResult, IExecutionStrategy defaultStrategy) {
		for (ParseResult subcommand : parseResult.subcommands()) {
			Object command = subcommand.commandSpec().userObject();
			if (AbstractImportCommand.class.isAssignableFrom(command.getClass())) {
				AbstractImportCommand importCommand = (AbstractImportCommand) command;
				for (ParseResult redisCommand : subcommand.subcommands()) {
					if (redisCommand.isUsageHelpRequested()) {
						return defaultStrategy.execute(redisCommand);
					}
					OperationCommand operationCommand = (OperationCommand) redisCommand.commandSpec().userObject();
					importCommand.getImportOperationCommands().add(operationCommand);
				}
				return new RunFirst().execute(subcommand);
			}
		}
		return defaultStrategy.execute(parseResult); // default execution strategy
	}

}
