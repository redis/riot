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

import com.redis.riot.cli.Main;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;

public class LoggingOptions {

	public static final Level DEFAULT_LEVEL = Level.SEVERE;
	public static final boolean DEFAULT_STACKTRACE = false;
	private static final String ROOT_LOGGER = "";

	private @Spec(MIXEE) CommandSpec mixee;

	private Level level = DEFAULT_LEVEL;
	private boolean stacktrace = DEFAULT_STACKTRACE;

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

	public static int executionStrategy(ParseResult parseResult) {
		getTopLevelCommandLoggingMixin(parseResult.commandSpec()).configureLoggers();
		return 0;
	}

	public void configureLoggers() {
		Level logLevel = getTopLevelCommandLoggingMixin(mixee).level;
		boolean printStacktrace = getTopLevelCommandLoggingMixin(mixee).stacktrace;
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		LogManager.getLogManager().reset();
		Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		handler.setFormatter(
				printStacktrace || logLevel.intValue() <= Level.INFO.intValue() ? new StackTraceOneLineLogFormat()
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