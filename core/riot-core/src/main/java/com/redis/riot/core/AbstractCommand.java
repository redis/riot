package com.redis.riot.core;

import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.slf4j.simple.SimpleLogger;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractCommand<C extends ExecutionContext> extends BaseCommand implements Callable<Integer> {

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit.")
	private boolean helpRequested;

	@ArgGroup(exclusive = false, heading = "Logging options%n")
	private LoggingArgs loggingArgs = new LoggingArgs();

	protected Logger log;

	@Override
	public Integer call() throws Exception {
		if (log == null) {
			setupLogging();
			log = LoggerFactory.getLogger(getClass());
		}
		try (C context = executionContext()) {
			context.afterPropertiesSet();
			execute(context);
		}
		return 0;
	}

	protected abstract C executionContext();

	private void setupLogging() {
		Level level = logLevel();
		System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, level.name());
		if (loggingArgs.getFile() != null) {
			System.setProperty(SimpleLogger.LOG_FILE_KEY, loggingArgs.getFile());
		}
		setBoolean(SimpleLogger.SHOW_DATE_TIME_KEY, loggingArgs.isShowDateTime());
		if (loggingArgs.getDateTimeFormat() != null) {
			System.setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, loggingArgs.getDateTimeFormat());
		}
		setBoolean(SimpleLogger.SHOW_THREAD_ID_KEY, loggingArgs.isShowThreadId());
		setBoolean(SimpleLogger.SHOW_THREAD_NAME_KEY, loggingArgs.isShowThreadName());
		setBoolean(SimpleLogger.SHOW_LOG_NAME_KEY, loggingArgs.isShowLogName());
		setBoolean(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, loggingArgs.isShowShortLogName());
		setBoolean(SimpleLogger.LEVEL_IN_BRACKETS_KEY, loggingArgs.isLevelInBrackets());
		setLogLevel("com.amazonaws.internal", Level.ERROR);
		setLogLevel("org.springframework.batch.core.step.builder.FaultTolerantStepBuilder", Level.ERROR);
		setLogLevel("org.springframework.batch.core.step.item.ChunkMonitor", Level.ERROR);
		for (Entry<String, Level> entry : loggingArgs.getLevels().entrySet()) {
			setLogLevel(entry.getKey(), entry.getValue());
		}
	}

	public Level logLevel() {
		return loggingArgs.level();
	}

	private static void setLogLevel(String key, Level level) {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + key, level.name());
	}

	private static void setBoolean(String property, boolean value) {
		System.setProperty(property, String.valueOf(value));
	}

	protected abstract void execute(C context);

	public LoggingArgs getLoggingArgs() {
		return loggingArgs;
	}

	public void setLoggingArgs(LoggingArgs args) {
		this.loggingArgs = args;
	}

	public Logger getLog() {
		return log;
	}

	public void setLog(Logger log) {
		this.log = log;
	}

}
