package com.redis.riot;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import io.lettuce.core.RedisURI;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;

@Data
@EqualsAndHashCode(callSuper = true)
@Command(sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = GenerateCompletionCommand.class, abbreviateSynopsis = true)
public class RiotApp extends HelpCommand {

	private static final String ROOT_LOGGER = "";

	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
	private boolean versionRequested;
	@ArgGroup(heading = "Redis connection options%n", exclusive = false)
	private RedisOptions redisOptions = new RedisOptions();
	@Option(names = { "-q", "--quiet" }, description = "Log errors only.")
	private boolean quiet;
	@Option(names = { "-w", "--warn" }, description = "Set log level to warn.")
	private boolean warning;
	@Option(names = { "-i", "--info" }, description = "Set log level to info.")
	private boolean info;
	@Option(names = { "-d", "--debug" }, description = "Log in debug mode (includes normal stacktrace).")
	private boolean debug;
	@Option(names = "--stacktrace", description = "Print out the stacktrace for all exceptions.")
	private boolean stacktrace;

	@SuppressWarnings("deprecation")
	private int executionStrategy(ParseResult parseResult) {
		configureLogging();
		return new CommandLine.RunLast().execute(parseResult); // default execution strategy
	}

	@SuppressWarnings("deprecation")
	private int executionStragegyRunFirst(ParseResult parseResult) {
		configureLogging();
		return new CommandLine.RunFirst().execute(parseResult);
	}

	private void configureLogging() {
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		LogManager.getLogManager().reset();
		Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		handler.setFormatter(debug || stacktrace ? new StackTraceOneLineLogFormat() : new OneLineLogFormat());
		activeLogger.addHandler(handler);
		Logger.getLogger(ROOT_LOGGER).setLevel(logLevel());
		Logger.getLogger("com.redis.riot").setLevel(riotLogLevel());
		Logger.getLogger("org.springframework.batch.item.redis").setLevel(riotLogLevel());
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
		return commandLine;
	}

	private Level logLevel() {
		if (debug) {
			return Level.FINE;
		}
		if (info) {
			return Level.INFO;
		}
		if (warning) {
			return Level.SEVERE;
		}
		if (quiet) {
			return Level.OFF;
		}
		return Level.WARNING;
	}

	private Level riotLogLevel() {
		if (debug) {
			return Level.FINER;
		}
		if (info) {
			return Level.FINE;
		}
		if (warning) {
			return Level.WARNING;
		}
		if (quiet) {
			return Level.SEVERE;
		}
		return Level.INFO;
	}

	private int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {
		// bold red error message
		cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
		return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
				: cmd.getCommandSpec().exitCodeOnExecutionException();
	}

	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(RedisURI.class, RedisURI::create);
		SpelExpressionParser parser = new SpelExpressionParser();
		commandLine.registerConverter(Expression.class, parser::parseExpression);
	}

}
