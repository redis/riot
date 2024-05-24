package com.redis.riot.core;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.event.Level;

import picocli.CommandLine.Option;

public class LoggingArgs {

	@Option(names = "--log-file", description = "Log output target. Can be a path or special values System.out and System.err (default: System.err).", paramLabel = "<file>")
	private String file;

	@Option(names = "--log-time", description = "Include current date and time in log messages.")
	private boolean showDateTime;

	@Option(names = "--log-time-format", description = "Date and time format to be used in log messages (default: ${DEFAULT-VALUE}). Use with --log-time.", paramLabel = "<f>")
	private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";

	@Option(names = "--log-thread-id", description = "Include current thread ID in log messages.", hidden = true)
	private boolean showThreadId;

	@Option(names = "--log-thread", description = "Include current thread name in log messages. True by default", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	private boolean showThreadName = true;

	@Option(names = "--log-name", description = "Include logger instance name in log messages. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	private boolean showLogName = true;

	@Option(names = "--log-short", description = "Include last component of logger instance name in log messages.", hidden = true)
	private boolean showShortLogName;

	@Option(names = "--log-level-bkt", description = "Output log level string in brackets.", hidden = true)
	private boolean levelInBrackets;

	@Option(arity = "1..*", names = "--log", description = "Custom log levels (e.g.: io.lettuce=INFO).", paramLabel = "<lvl>")
	private Map<String, Level> levels = new LinkedHashMap<>();

	@Option(names = { "-d", "--debug" }, description = "Log in debug mode.")
	private boolean debug;

	@Option(names = { "-i", "--info" }, description = "Set log level to info.")
	private boolean info;

	@Option(names = { "-w", "--warn" }, description = "Set log level to warn.")
	private boolean warn;

	@Option(names = { "-q", "--quiet" }, description = "Log errors only.")
	private boolean quiet;

	public Level level(Level defaultLevel) {
		if (debug) {
			return Level.DEBUG;
		}
		if (info) {
			return Level.INFO;
		}
		if (warn) {
			return Level.WARN;
		}
		if (quiet) {
			return Level.ERROR;
		}
		return defaultLevel;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String logFile) {
		this.file = logFile;
	}

	public boolean isShowDateTime() {
		return showDateTime;
	}

	public void setShowDateTime(boolean showDateTime) {
		this.showDateTime = showDateTime;
	}

	public String getDateTimeFormat() {
		return dateTimeFormat;
	}

	public void setDateTimeFormat(String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
	}

	public boolean isShowThreadId() {
		return showThreadId;
	}

	public void setShowThreadId(boolean showThreadId) {
		this.showThreadId = showThreadId;
	}

	public boolean isShowThreadName() {
		return showThreadName;
	}

	public void setShowThreadName(boolean showThreadName) {
		this.showThreadName = showThreadName;
	}

	public boolean isShowLogName() {
		return showLogName;
	}

	public void setShowLogName(boolean showLogName) {
		this.showLogName = showLogName;
	}

	public boolean isShowShortLogName() {
		return showShortLogName;
	}

	public void setShowShortLogName(boolean showShortLogName) {
		this.showShortLogName = showShortLogName;
	}

	public boolean isLevelInBrackets() {
		return levelInBrackets;
	}

	public void setLevelInBrackets(boolean levelInBrackets) {
		this.levelInBrackets = levelInBrackets;
	}

	public Map<String, Level> getLevels() {
		return levels;
	}

	public void setLevels(Map<String, Level> logLevels) {
		this.levels = logLevels;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public boolean isInfo() {
		return info;
	}

	public void setInfo(boolean info) {
		this.info = info;
	}

	public boolean isWarn() {
		return warn;
	}

	public void setWarn(boolean warn) {
		this.warn = warn;
	}

	public boolean isQuiet() {
		return quiet;
	}

	public void setQuiet(boolean quiet) {
		this.quiet = quiet;
	}

}
