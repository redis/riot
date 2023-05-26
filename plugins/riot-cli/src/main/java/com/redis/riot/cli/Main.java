package com.redis.riot.cli;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redis.riot.cli.common.DoubleRangeTypeConverter;
import com.redis.riot.cli.common.HelpOptions;
import com.redis.riot.cli.common.IntRangeTypeConverter;
import com.redis.riot.cli.common.LoggingOptions;
import com.redis.riot.cli.common.ManifestVersionProvider;
import com.redis.riot.cli.common.RedisOptions;
import com.redis.riot.cli.common.RiotExecutionStrategy;
import com.redis.spring.batch.common.DoubleRange;
import com.redis.spring.batch.common.IntRange;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;

@Command(name = "riot", usageHelpAutoWidth = true, versionProvider = ManifestVersionProvider.class, subcommands = {
		DbImport.class, DbExport.class, DumpImport.class, FileImport.class, FileExport.class, FakerImport.class,
		Generate.class, Replicate.class, Ping.class, GenerateCompletion.class })
public class Main {

	@Mixin
	private HelpOptions helpOptions = new HelpOptions();

	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
	private boolean versionRequested;

	@Mixin
	private LoggingOptions loggingOptions = new LoggingOptions();

	@ArgGroup(heading = "Redis connection options%n", exclusive = false)
	private RedisOptions redisOptions = new RedisOptions();

	public static void main(String[] args) {
		System.exit(new Main().execute(args));
	}

	public RedisOptions getRedisOptions() {
		return redisOptions;
	}

	public LoggingOptions getLoggingOptions() {
		return loggingOptions;
	}

	public int execute(String... args) {
		return commandLine().execute(args);
	}

	public CommandLine commandLine() {
		CommandLine commandLine = new CommandLine(this);
		commandLine.setExecutionStrategy(new RiotExecutionStrategy(LoggingOptions::executionStrategy));
		commandLine.setExecutionExceptionHandler(this::handleExecutionException);
		registerConverters(commandLine);
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
		return commandLine;
	}

	private int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {
		// bold red error message
		cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
		return cmd.getExitCodeExceptionMapper() == null ? cmd.getCommandSpec().exitCodeOnExecutionException()
				: cmd.getExitCodeExceptionMapper().getExitCode(ex);
	}

	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(RedisURI.class, RedisURI::create);
		commandLine.registerConverter(IntRange.class, new IntRangeTypeConverter());
		commandLine.registerConverter(DoubleRange.class, new DoubleRangeTypeConverter());
		SpelExpressionParser parser = new SpelExpressionParser();
		commandLine.registerConverter(Expression.class, parser::parseExpression);
		commandLine.registerConverter(ReadFrom.class, ReadFrom::valueOf);
	}

}
