package com.redis.riot.test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;
import java.util.Vector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;

import com.redis.riot.core.MainCommand;
import com.redis.riot.core.RiotDuration;
import com.redis.spring.batch.test.AbstractTargetTestBase;

import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;

public abstract class AbstractRiotTestBase extends AbstractTargetTestBase {

	public static final RiotDuration DEFAULT_IDLE_TIMEOUT = RiotDuration.ofSeconds(1);
	public static final int DEFAULT_EVENT_QUEUE_CAPACITY = 100000;

	static {
		System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
		System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "info");
	}

	protected static void assertExecutionSuccessful(int exitCode) {
		Assertions.assertEquals(0, exitCode);
	}

	protected <T> T command(ParseResult parseResult) {
		return parseResult.subcommands().get(0).commandSpec().commandLine().getCommand();
	}

	protected int execute(TestInfo info, String filename) throws Exception {
		return doExecute(info, filename);
	}

	protected int execute(TestInfo info, String filename, IExecutionStrategy config) throws Exception {
		return doExecute(info, filename, config);
	}

	private int doExecute(TestInfo info, String filename, IExecutionStrategy... executionStrategies) throws Exception {
		MainCommand mainCommand = mainCommand(info, executionStrategies);
		return mainCommand.run(args(mainCommand, filename));
	}

	protected abstract MainCommand mainCommand(TestInfo info, IExecutionStrategy... executionStrategies);

	protected abstract String getMainCommandPrefix();

	private String[] args(MainCommand mainCommand, String filename) throws Exception {
		try (InputStream inputStream = mainCommand.getClass().getResourceAsStream("/" + filename)) {
			String command = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
			String prefix = getMainCommandPrefix();
			if (command.startsWith(prefix)) {
				command = command.substring(prefix.length());
			}
			return translateCommandline(command);
		}
	}

	protected String[] translateCommandline(String toProcess) throws Exception {
		if ((toProcess == null) || (toProcess.length() == 0)) {
			return new String[0];
		}

		// parse with a simple finite state machine

		final int normal = 0;
		final int inQuote = 1;
		final int inDoubleQuote = 2;
		int state = normal;
		StringTokenizer tok = new StringTokenizer(toProcess, "\"\' ", true);
		Vector<String> v = new Vector<String>();
		StringBuilder current = new StringBuilder();

		while (tok.hasMoreTokens()) {
			String nextTok = tok.nextToken();
			switch (state) {
			case inQuote:
				if ("\'".equals(nextTok)) {
					state = normal;
				} else {
					current.append(nextTok);
				}
				break;
			case inDoubleQuote:
				if ("\"".equals(nextTok)) {
					state = normal;
				} else {
					current.append(nextTok);
				}
				break;
			default:
				if ("\'".equals(nextTok)) {
					state = inQuote;
				} else if ("\"".equals(nextTok)) {
					state = inDoubleQuote;
				} else if (" ".equals(nextTok)) {
					if (current.length() != 0) {
						v.addElement(current.toString());
						current.setLength(0);
					}
				} else {
					current.append(nextTok);
				}
				break;
			}
		}

		if (current.length() != 0) {
			v.addElement(current.toString());
		}

		if ((state == inQuote) || (state == inDoubleQuote)) {
			throw new Exception("unbalanced quotes in " + toProcess);
		}

		String[] args = new String[v.size()];
		v.copyInto(args);
		return args;
	}

}
