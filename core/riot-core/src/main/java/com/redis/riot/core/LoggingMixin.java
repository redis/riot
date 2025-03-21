package com.redis.riot.core;

import static picocli.CommandLine.Spec.Target.MIXEE;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.event.Level;
import org.slf4j.simple.SimpleLogger;

import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;

public class LoggingMixin {

	public static final boolean DEFAULT_SHOW_DATE_TIME = true;
	public static final boolean DEFAULT_SHOW_THREAD_NAME = true;
	public static final boolean DEFAULT_SHOW_LOG_NAME = true;
	public static final Level DEFAULT_LEVEL = Level.WARN;

	/**
	 * This mixin is able to climb the command hierarchy because the
	 * {@code @Spec(Target.MIXEE)}-annotated field gets a reference to the command
	 * where it is used.
	 */
	private @Spec(MIXEE) CommandSpec mixee; // spec of the command where the @Mixin is used

	private String file;
	private boolean showDateTime = DEFAULT_SHOW_DATE_TIME;
	private String dateTimeFormat;
	private boolean showThreadId;
	private boolean showThreadName = DEFAULT_SHOW_THREAD_NAME;
	private boolean showLogName = DEFAULT_SHOW_LOG_NAME;
	private boolean showShortLogName;
	private boolean levelInBrackets;
	private Map<String, Level> levels = new LinkedHashMap<>();
	private Level level = DEFAULT_LEVEL;

	private static LoggingMixin getTopLevelCommandLoggingMixin(CommandSpec commandSpec) {
		return ((MainCommand) commandSpec.root().userObject()).loggingMixin;
	}

	@Option(names = "--log-file", description = "Log output target. Can be a path or special values System.out and System.err (default: System.err).", paramLabel = "<file>")
	public void setFile(String file) {
		getTopLevelCommandLoggingMixin(mixee).file = file;
	}

	@Option(names = "--log-time", description = "Include current date and time in log messages. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	public void setShowDateTime(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showDateTime = show;
	}

	@Option(names = "--log-time-fmt", defaultValue = "yyyy-MM-dd HH:mm:ss.SSS", description = "Date and time format to be used in log messages (default: ${DEFAULT-VALUE}). Use with --log-time.", paramLabel = "<f>")
	public void setDateTimeFormat(String format) {
		getTopLevelCommandLoggingMixin(mixee).dateTimeFormat = format;
	}

	@Option(names = "--log-thread-id", description = "Include current thread ID in log messages.", hidden = true)
	public void setShowThreadId(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showThreadId = show;
	}

	@Option(names = "--log-thread", description = "Show current thread name in log messages. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	public void setShowThreadName(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showThreadName = show;
	}

	@Option(names = "--log-name", description = "Show logger instance name in log messages. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	public void setShowLogName(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showLogName = show;
	}

	@Option(names = "--log-short", description = "Include last component of logger instance name in log messages. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	public void setShowShortLogName(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showShortLogName = show;
	}

	@Option(names = "--log-level-brck", description = "Output log level string in brackets.", hidden = true)
	public void setLevelInBrackets(boolean enable) {
		getTopLevelCommandLoggingMixin(mixee).levelInBrackets = enable;
	}

	@Option(arity = "1..*", names = "--log", description = "Custom log levels (e.g.: io.lettuce=INFO).", paramLabel = "<lvl>")
	public void setLevels(Map<String, Level> levels) {
		getTopLevelCommandLoggingMixin(mixee).levels = levels;
	}

	@Option(names = { "-d", "--debug" }, description = "Log in debug mode.")
	public void setDebug(boolean enable) {
		if (enable) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.DEBUG;
		}
	}

	@Option(names = { "-i", "--info" }, description = "Set log level to info.")
	public void setInfo(boolean enable) {
		if (enable) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.INFO;
		}
	}

	@Option(names = { "-q", "--quiet" }, description = "Log errors only.")
	public void setError(boolean enable) {
		if (enable) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.ERROR;
		}
	}

	@Option(names = "--log-level", description = "Set log level: ${COMPLETION-CANDIDATES} (default: WARN).", paramLabel = "<lvl>", hidden = true)
	public void setLevel(Level level) {
		getTopLevelCommandLoggingMixin(mixee).level = level;
	}

	public static int executionStrategy(ParseResult parseResult) {
		getTopLevelCommandLoggingMixin(parseResult.commandSpec()).configureLoggers();
		return 0;
	}

	public void configureLoggers() {
		LoggingMixin mixin = getTopLevelCommandLoggingMixin(mixee);
		System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, mixin.level.name());
		if (mixin.file != null) {
			System.setProperty(SimpleLogger.LOG_FILE_KEY, mixin.file);
		}
		setBoolean(SimpleLogger.SHOW_DATE_TIME_KEY, mixin.showDateTime);
		if (mixin.dateTimeFormat != null) {
			System.setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, mixin.dateTimeFormat);
		}
		setBoolean(SimpleLogger.SHOW_THREAD_ID_KEY, mixin.showThreadId);
		setBoolean(SimpleLogger.SHOW_THREAD_NAME_KEY, mixin.showThreadName);
		setBoolean(SimpleLogger.SHOW_LOG_NAME_KEY, mixin.showLogName);
		setBoolean(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, mixin.showShortLogName);
		setBoolean(SimpleLogger.LEVEL_IN_BRACKETS_KEY, mixin.levelInBrackets);
		setLogLevel("com.amazonaws.internal", Level.ERROR);
		setLogLevel("com.redis.spring.batch.step.FlushingFaultTolerantStepBuilder", Level.ERROR);
		setLogLevel("org.springframework.batch.core.step.builder.FaultTolerantStepBuilder", Level.ERROR);
		setLogLevel("org.springframework.batch.core.step.item.ChunkMonitor", Level.ERROR);
		mixin.levels.forEach(this::setLogLevel);
	}

	private void setLogLevel(String key, Level level) {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + key, level.name());
	}

	private void setBoolean(String property, boolean value) {
		System.setProperty(property, String.valueOf(value));
	}

	public boolean isStacktrace() {
		return level.toInt() <= Level.INFO.toInt();
	}

}