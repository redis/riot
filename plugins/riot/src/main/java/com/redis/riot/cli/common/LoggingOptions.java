package com.redis.riot.cli.common;

import static picocli.CommandLine.Spec.Target.MIXEE;

import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.redis.riot.cli.Main;
import com.redis.riot.core.logging.RiotLevel;
import com.redis.riot.core.logging.SingleLineFormatter;
import com.redis.riot.core.logging.StackTraceSingleLineFormatter;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;

public class LoggingOptions {

	public static final Level DEFAULT_LEVEL = RiotLevel.LIFECYCLE;
	private static final Level DEFAULT_DEPENDENCY_LEVEL = Level.SEVERE;
	public static final boolean DEFAULT_STACKTRACE = false;
	private static final String ROOT_LOGGER = "";

	private @Spec(MIXEE) CommandSpec mixee;

	private Level level = DEFAULT_LEVEL;
	private boolean stacktrace = DEFAULT_STACKTRACE;
	@Option(names = "--dep-log-level", description = "Set log level for dependencies", hidden = true)
	private Level dependencyLevel = DEFAULT_DEPENDENCY_LEVEL;

	private static LoggingOptions getTopLevelCommandLoggingMixin(CommandSpec commandSpec) {
		return ((Main) commandSpec.root().userObject()).getLoggingOptions();
	}

	@Option(names = { "-d", "--debug" }, description = "Log in debug mode (includes normal stacktrace).")
	public void setDebug(boolean debug) {
		if (debug) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.FINE;
		}
	}

	@Option(names = { "-i", "--info" }, description = "Set log level to info.")
	public void setInfo(boolean info) {
		if (info) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.INFO;
		}
	}

	@Option(names = { "-q", "--quiet" }, description = "Log errors only.")
	public void setQuiet(boolean quiet) {
		if (quiet) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.SEVERE;
		}
	}

	@Option(names = { "-w", "--warn" }, description = "Set log level to warn.")
	public void setWarning(boolean warning) {
		if (warning) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.WARNING;
		}
	}

	@Option(names = "--stacktrace", description = "Print out the stacktrace for all exceptions.")
	public void setStacktrace(boolean stacktrace) {
		getTopLevelCommandLoggingMixin(mixee).stacktrace = stacktrace;
	}

	/**
	 * Returns the verbosity from the LoggingMixin of the top-level command.
	 * 
	 * @return the verbosity value
	 */
	public Level getLevel() {
		return getTopLevelCommandLoggingMixin(mixee).level;
	}

	public static int executionStrategy(ParseResult parseResult) {
		getTopLevelCommandLoggingMixin(parseResult.commandSpec()).configureLoggers();
		return 0;
	}

	public void configureLoggers() {
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		LogManager.getLogManager().reset();
		Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		handler.setFormatter(formatter());
		activeLogger.addHandler(handler);
		Level logLevel = logLevel();
		Logger.getLogger(ROOT_LOGGER).setLevel(logLevel);
		Logger.getLogger("com.amazonaws").setLevel(dependencyLevel());
		Logger.getLogger("io.lettuce").setLevel(dependencyLevel());
		Logger.getLogger("org.springframework").setLevel(dependencyLevel());
	}

	private Level dependencyLevel() {
		Level logLevel = logLevel();
		if (logLevel == DEFAULT_LEVEL) {
			return dependencyLevel;
		}
		return min(dependencyLevel, logLevel);
	}

	private Level min(Level level1, Level level2) {
		if (level1.intValue() < level2.intValue()) {
			return level1;
		}
		return level2;
	}

	private Level logLevel() {
		return getTopLevelCommandLoggingMixin(mixee).level;
	}

	private Formatter formatter() {
		if (isStacktraceEnabled()) {
			return new StackTraceSingleLineFormatter();
		}
		return new SingleLineFormatter();
	}

	private boolean isStacktraceEnabled() {
		return getTopLevelCommandLoggingMixin(mixee).stacktrace || isVerbose();
	}

	private boolean isVerbose() {
		return !isGreaterOrEqual(Level.CONFIG);
	}

	public boolean isGreaterOrEqual(Level level) {
		return logLevel().intValue() >= level.intValue();
	}

}