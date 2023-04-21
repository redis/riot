package com.redis.riot.cli;

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

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import picocli.CommandLine.Option;

public class LoggingOptions {

	private static final String ROOT_LOGGER = "";

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

	public Level logLevel() {
		if (debug) {
			return Level.FINE;
		}
		if (info) {
			return Level.INFO;
		}
		if (warning) {
			return Level.WARNING;
		}
		if (quiet) {
			return Level.OFF;
		}
		return Level.SEVERE;
	}

	public void configure() {
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		LogManager.getLogManager().reset();
		Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		handler.setFormatter(stacktrace || debug ? new StackTraceOneLineLogFormat() : new OneLineLogFormat());
		activeLogger.addHandler(handler);
		Logger.getLogger(ROOT_LOGGER).setLevel(logLevel());
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public void setInfo(boolean info) {
		this.info = info;
	}

	public void setQuiet(boolean quiet) {
		this.quiet = quiet;
	}

	public void setWarning(boolean warning) {
		this.warning = warning;
	}

	public void setStacktrace(boolean stacktrace) {
		this.stacktrace = stacktrace;
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
