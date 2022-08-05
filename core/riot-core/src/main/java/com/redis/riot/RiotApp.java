package com.redis.riot;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redis.spring.batch.support.DoubleRange;
import com.redis.spring.batch.support.IntRange;

import io.lettuce.core.RedisURI;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

@Command(sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = GenerateCompletionCommand.class, abbreviateSynopsis = true)
public class RiotApp extends HelpCommand {

	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
	private boolean versionRequested;

	@ArgGroup(heading = "Redis connection options%n", exclusive = false)
	private RedisOptions redisOptions = new RedisOptions();

	@ArgGroup(heading = "Logging options%n", exclusive = false)
	private LoggingOptions loggingOptions = new LoggingOptions();

	public RedisOptions getRedisOptions() {
		return redisOptions;
	}

	public LoggingOptions getLoggingOptions() {
		return loggingOptions;
	}

	protected int execute(IExecutionStrategy strategy, ParseResult parseResult) {
		configureLogging();
		Logger log = Logger.getLogger(getClass().getName());
		log.log(Level.FINE, "Running {0} {1} with {2}", new Object[] { parseResult.commandSpec().name(),
				ManifestVersionProvider.getVersionString(), parseResult.originalArgs() });
		return strategy.execute(parseResult);
	}

	protected void configureLogging() {
		loggingOptions.configure();
	}

	public int execute(String... args) {
		return commandLine().execute(args);
	}

	public RiotCommandLine commandLine() {
		RiotCommandLine commandLine = new RiotCommandLine(this, r -> execute(new RunFirst(), r));
		commandLine.setExecutionStrategy(r -> execute(new RunLast(), r));
		commandLine.setExecutionExceptionHandler(this::handleExecutionException);
		registerConverters(commandLine);
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
		return commandLine;
	}

	private int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {
		// bold red error message
		cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
		return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
				: cmd.getCommandSpec().exitCodeOnExecutionException();
	}

	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(RedisURI.class, RedisURI::create);
		commandLine.registerConverter(IntRange.class, new IntRangeTypeConverter());
		commandLine.registerConverter(DoubleRange.class, new DoubleRangeTypeConverter());
		SpelExpressionParser parser = new SpelExpressionParser();
		commandLine.registerConverter(Expression.class, parser::parseExpression);
	}

}
