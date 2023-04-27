package com.redis.riot.cli.common;

import static picocli.CommandLine.Spec.Target.MIXEE;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.TimeZone;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.springframework.core.NestedExceptionUtils;

import com.redis.riot.cli.Riot;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;

public class LoggingOptions {

	private static final String ROOT_LOGGER = "";

	/**
	 * This mixin is able to climb the command hierarchy because the
	 * {@code @Spec(Target.MIXEE)}-annotated field gets a reference to the command
	 * where it is used.
	 */
	private @Spec(MIXEE) CommandSpec mixee; // spec of the command where the @Mixin is used

	private Level level = Level.SEVERE;
	private boolean stacktrace;

	// Each subcommand that mixes in the LoggingMixin has its own instance of this
	// class,
	// so there may be many LoggingMixin instances.
	// We want to store the verbosity value in a single, central place, so
	// we find the top-level command,
	// and store the verbosity level on our top-level command's LoggingMixin.
	//
	// In the main method, `LoggingMixin::executionStrategy` should be set as the
	// execution strategy:
	// that will take the verbosity level that we stored in the top-level command's
	// LoggingMixin
	// to configure Log4j2 before executing the command that the user specified.
	private static LoggingOptions getTopLevelCommandLoggingMixin(CommandSpec commandSpec) {
		return ((Riot) commandSpec.root().userObject()).getLoggingOptions();
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
			getTopLevelCommandLoggingMixin(mixee).level = Level.OFF;
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

	/**
	 * Configures Log4j2 based on the verbosity level of the top-level command's
	 * LoggingMixin, before invoking the default execution strategy
	 * ({@link picocli.CommandLine.RunLast RunLast}) and returning the result.
	 * <p>
	 * Example usage:
	 * </p>
	 * 
	 * <pre>
	 * public void main(String... args) {
	 *     new CommandLine(new MyApp())
	 *             .setExecutionStrategy(LoggingMixin::executionStrategy))
	 *             .execute(args);
	 * }
	 * </pre>
	 *
	 * @param parseResult represents the result of parsing the command line
	 * @return the exit code of executing the most specific subcommand
	 */
	public static int executionStrategy(ParseResult parseResult) {
		getTopLevelCommandLoggingMixin(parseResult.commandSpec()).configureLoggers();
		return 0;
	}

	/**
	 * Configures the Log4j2 console appender(s), using the specified verbosity:
	 * <ul>
	 * <li>{@code -vvv} : enable TRACE level</li>
	 * <li>{@code -vv} : enable DEBUG level</li>
	 * <li>{@code -v} : enable INFO level</li>
	 * <li>(not specified) : enable WARN level</li>
	 * </ul>
	 */
	public void configureLoggers() {
		Level logLevel = getTopLevelCommandLoggingMixin(mixee).level;
		boolean printStacktrace = getTopLevelCommandLoggingMixin(mixee).stacktrace;
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		LogManager.getLogManager().reset();
		Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		handler.setFormatter(
				printStacktrace || logLevel.intValue() <= Level.FINE.intValue() ? new StackTraceOneLineLogFormat()
						: new OneLineLogFormat());
		activeLogger.addHandler(handler);
		Logger.getLogger(ROOT_LOGGER).setLevel(logLevel);
		Logger.getLogger("com.amazonaws").setLevel(Level.SEVERE);
	}

	static class OneLineLogFormat extends Formatter {

		@Override
		public String format(LogRecord logRecord) {
			String message = formatMessage(logRecord);
			if (logRecord.getThrown() != null) {
				Throwable rootCause = NestedExceptionUtils.getRootCause(logRecord.getThrown());
				if (rootCause != null && rootCause.getMessage() != null) {
					return String.format("%s: %s%n", message, rootCause.getMessage());
				}
			}
			return String.format("%s%n", message);
		}

	}

	static class StackTraceOneLineLogFormat extends Formatter {

		private final DateTimeFormatter d = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
				.appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).optionalStart().appendLiteral(':')
				.appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart()
				.appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true).toFormatter();

		private final ZoneId offset = TimeZone.getDefault().toZoneId();

		@Override
		public String format(LogRecord logRecord) {
			String message = formatMessage(logRecord);
			ZonedDateTime time = Instant.ofEpochMilli(logRecord.getMillis()).atZone(offset);
			if (logRecord.getThrown() == null) {
				return String.format("%s %s %s\t: %s%n", time.format(d), logRecord.getLevel().getLocalizedName(),
						logRecord.getLoggerName(), message);
			}
			return String.format("%s %s %s\t: %s%n%s%n", time.format(d), logRecord.getLevel().getLocalizedName(),
					logRecord.getLoggerName(), message, stackTrace(logRecord));
		}

		private String stackTrace(LogRecord logRecord) {
			StringWriter sw = new StringWriter(4096);
			PrintWriter pw = new PrintWriter(sw);
			logRecord.getThrown().printStackTrace(pw);
			return sw.toString();
		}
	}

}