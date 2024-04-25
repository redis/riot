package com.redis.riot.cli;

import static java.lang.System.setProperty;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.event.Level;
import org.slf4j.simple.SimpleLogger;

import picocli.CommandLine.Option;

public class LoggingArgs {

	private Level level = Level.WARN;
	@Option(names = "--log-file", description = "Log output target. Can be a path or special values System.out and System.err (default: System.err).", paramLabel = "<file>")
	private String logFile;
	@Option(names = "--log-time", description = "Include current date and time in log messages.")
	private boolean showDateTime;
	@Option(names = "--log-time-format", description = "Date and time format to be used in log messages (default: ${DEFAULT-VALUE}). Use with --log-time.", paramLabel = "<f>")
	private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";
	@Option(names = "--log-thread-id", description = "Include current thread ID in log messages.", hidden = true)
	private boolean showThreadId;
	@Option(names = "--log-thread-name", description = "Include current thread name in log messages.", hidden = true)
	private boolean showThreadName;
	@Option(names = "--log-name", description = "Include logger instance name in log messages.", hidden = true)
	private boolean showLogName;
	@Option(names = "--log-short", description = "Include last component of logger instance name in log messages.", hidden = true)
	private boolean showShortLogName;
	@Option(names = "--log-level", description = "Output log level string in brackets.", hidden = true)
	private boolean levelInBrackets;
	@Option(arity = "1..*", names = "--log", description = "Custom log levels (e.g.: io.lettuce=INFO).", paramLabel = "<lvl>")
	private Map<String, Level> logLevels = new LinkedHashMap<>();

	@Option(names = { "-d", "--debug" }, description = "Log in debug mode.")
	public void setDebug(boolean debug) {
		this.level = Level.DEBUG;
	}

	@Option(names = { "-i", "--info" }, description = "Set log level to info.")
	public void setInfo(boolean info) {
		this.level = Level.INFO;
	}

	@Option(names = { "-q", "--quiet" }, description = "Log errors only.")
	public void setError(boolean error) {
		this.level = Level.ERROR;
	}

	public void configureLogging() {
		setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, level.name());
		if (logFile != null) {
			setProperty(SimpleLogger.LOG_FILE_KEY, logFile);
		}
		setBoolean(SimpleLogger.SHOW_DATE_TIME_KEY, showDateTime);
		if (dateTimeFormat != null) {
			setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, dateTimeFormat);
		}
		setBoolean(SimpleLogger.SHOW_THREAD_ID_KEY, showThreadId);
		setBoolean(SimpleLogger.SHOW_THREAD_NAME_KEY, showThreadName);
		setBoolean(SimpleLogger.SHOW_LOG_NAME_KEY, showLogName);
		setBoolean(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, showShortLogName);
		setBoolean(SimpleLogger.LEVEL_IN_BRACKETS_KEY, levelInBrackets);
		setLogLevel("com.amazonaws.internal", Level.ERROR);
		setLogLevel("org.springframework.batch.core.step.builder.FaultTolerantStepBuilder", Level.ERROR);
		setLogLevel("org.springframework.batch.core.step.item.ChunkMonitor", Level.ERROR);
		for (Entry<String, Level> entry : logLevels.entrySet()) {
			setLogLevel(entry.getKey(), entry.getValue());
		}
	}

	private static void setLogLevel(String key, Level level) {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + key, level.name());
	}

	private static void setBoolean(String property, boolean value) {
		if (value) {
			setProperty(property, String.valueOf(value));
		}
	}

}
