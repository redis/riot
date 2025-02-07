package com.redis.riot.core;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractCallableCommand extends BaseCommand implements Callable<Integer> {

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit.")
	private boolean helpRequested;

	protected Logger log;

	@Override
	public Integer call() throws Exception {
		initialize();
		try {
			execute();
		} finally {
			teardown();
		}
		return 0;
	}

	protected void initialize() {
		if (log == null) {
			log = LoggerFactory.getLogger(getClass());
		}
	}

	protected abstract void execute();

	protected void teardown() {
		// do nothing
	}

	public Logger getLog() {
		return log;
	}

	public void setLog(Logger log) {
		this.log = log;
	}

}
