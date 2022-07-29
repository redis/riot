package com.redis.riot;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redis.spring.batch.support.DoubleRange;
import com.redis.spring.batch.support.IntRange;

import io.lettuce.core.RedisURI;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

@Command(sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = GenerateCompletionCommand.class, abbreviateSynopsis = true)
public class RiotApp extends HelpCommand {

	private static final org.slf4j.Logger log = LoggerFactory.getLogger(RiotApp.class);

	private static final String ROOT_LOGGER = "";

	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
	private boolean versionRequested;
	@ArgGroup(heading = "Redis connection options%n", exclusive = false)
	private RedisOptions redisOptions = new RedisOptions();
	@Mixin
	private LoggingOptions loggingOptions = new LoggingOptions();

	public RedisOptions getRedisOptions() {
		return redisOptions;
	}

	public LoggingOptions getLoggingOptions() {
		return loggingOptions;
	}

	private int executionStrategy(ParseResult parseResult) {
		return execute(new RunLast(), parseResult); // default execution strategy
	}

	private int executionStragegyRunFirst(ParseResult parseResult) {
		return execute(new RunFirst(), parseResult);
	}

	private int execute(IExecutionStrategy strategy, ParseResult parseResult) {
		configureLogging();
		log.debug("Running {} {} with {}", parseResult.commandSpec().name(), ManifestVersionProvider.getVersionString(),
				parseResult.originalArgs());
		return strategy.execute(parseResult);
	}

	protected void configureLogging() {
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		LogManager.getLogManager().reset();
		Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		handler.setFormatter(loggingOptions.isStacktrace() ? new StackTraceOneLineLogFormat() : new OneLineLogFormat());
		activeLogger.addHandler(handler);
		Logger.getLogger(ROOT_LOGGER).setLevel(getLogLevel());
		Logger.getLogger("com.redis.riot").setLevel(getRiotLogLevel());
		Logger.getLogger("com.redis.spring.batch").setLevel(getRiotLogLevel());
		Logger.getLogger("org.springframework.batch.core.step.item.ChunkMonitor").setLevel(getSpringLevel());
		Logger.getLogger("org.springframework.batch.core.step.builder.FaultTolerantStepBuilder")
				.setLevel(getSpringLevel());
	}

	private Level getSpringLevel() {
		if (loggingOptions.isDebug()) {
			return Level.INFO;
		}
		if (loggingOptions.isInfo()) {
			return Level.WARNING;
		}
		if (loggingOptions.isWarning()) {
			return Level.SEVERE;
		}
		if (loggingOptions.isQuiet()) {
			return Level.OFF;
		}
		return Level.SEVERE;
	}

	private Level getLogLevel() {
		if (loggingOptions.isDebug()) {
			return Level.FINE;
		}
		if (loggingOptions.isInfo()) {
			return Level.INFO;
		}
		if (loggingOptions.isWarning()) {
			return Level.SEVERE;
		}
		if (loggingOptions.isQuiet()) {
			return Level.OFF;
		}
		return Level.WARNING;
	}

	private Level getRiotLogLevel() {
		if (loggingOptions.isDebug()) {
			return Level.FINEST;
		}
		if (loggingOptions.isInfo()) {
			return Level.FINE;
		}
		if (loggingOptions.isWarning()) {
			return Level.WARNING;
		}
		if (loggingOptions.isQuiet()) {
			return Level.SEVERE;
		}
		return Level.INFO;
	}

	public int execute(String... args) {
		return commandLine().execute(args);
	}

	public RiotCommandLine commandLine() {
		RiotCommandLine commandLine = new RiotCommandLine(this, this::executionStragegyRunFirst);
		commandLine.setExecutionStrategy(this::executionStrategy);
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
