package com.redis.riot;

import com.redis.riot.core.MainCommand;
import com.redis.spring.batch.item.redis.common.Range;

import io.lettuce.core.RedisURI;
import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionStrategy;
import software.amazon.awssdk.regions.Region;

@Command(name = "riot", versionProvider = Versions.class, headerHeading = "A data import/export tool for Redis.%n%n", footerHeading = "%nRun 'riot COMMAND --help' for more information on a command.%n%nFor more help on how to use RIOT, head to http://redis.github.io/riot%n", subcommands = {
		DatabaseExport.class, DatabaseImport.class, FakerImport.class, FileExport.class, FileImport.class,
		Generate.class, Ping.class, Replicate.class, Compare.class, GenerateCompletion.class })
public class Riot extends MainCommand {

	public static final IExecutionStrategy DEFAULT_EXECUTION_STRATEGY = new RiotExecutionStrategy();

	public static void main(String[] args) {
		System.exit(new Riot().run(args));
	}

	@Override
	protected CommandLine commandLine() {
		CommandLine commandLine = super.commandLine();
		commandLine.setExecutionStrategy(executionStrategy());
		configureCommandLine(commandLine);
		return commandLine;
	}

	public static void configureCommandLine(CommandLine commandLine) {
		commandLine.registerConverter(RedisURI.class, new RedisURIConverter());
		commandLine.registerConverter(Region.class, Region::of);
		commandLine.registerConverter(Range.class, new RangeConverter());
	}

	protected IExecutionStrategy executionStrategy() {
		return DEFAULT_EXECUTION_STRATEGY;
	}

}
