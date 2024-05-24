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
import picocli.CommandLine.ParentCommand;

@Command
public abstract class AbstractCommand extends BaseCommand implements Callable<Integer> {

	@ParentCommand
	protected AbstractMain parent;

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit.")
	private boolean helpRequested;

	@ArgGroup(exclusive = false, heading = "Logging options%n")
	private LoggingArgs loggingArgs = new LoggingArgs();

	protected Logger log;

	public void copyTo(AbstractCommand target) {
		target.helpRequested = helpRequested;
		target.loggingArgs = loggingArgs;
		target.log = log;
	}

	protected void setup() {
		setupLogging();
		log = LoggerFactory.getLogger(getClass());
	}

	private void setupLogging() {
		Level level = loggingArgs.level(Level.WARN);
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

	private static void setLogLevel(String key, Level level) {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + key, level.name());
	}

	private static void setBoolean(String property, boolean value) {
		System.setProperty(property, String.valueOf(value));
	}

	@Override
	public Integer call() throws Exception {
		setup();
		execute();
		return 0;
	}

	protected abstract void execute() throws Exception;

}
