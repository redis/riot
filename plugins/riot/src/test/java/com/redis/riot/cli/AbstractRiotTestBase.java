package com.redis.riot.cli;

import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.StringTokenizer;
import java.util.Vector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;

import com.redis.riot.cli.AbstractRiotCommand.ProgressStyle;
import com.redis.riot.redis.CompareMode;
import com.redis.riot.redis.Replication.LoggingWriteListener;
import com.redis.riot.redis.ReplicationMode;
import com.redis.spring.batch.test.AbstractTargetTestBase;

import io.micrometer.core.instrument.util.IOUtils;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;

abstract class AbstractRiotTestBase extends AbstractTargetTestBase {

	private static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 100000;
	private static final String PREFIX = "riot ";

	private PrintWriter out = new PrintWriter(System.out);
	private PrintWriter err = new PrintWriter(System.err);

	static {
		System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + LoggingWriteListener.class.getName(), "error");
	}

	protected static void assertExecutionSuccessful(int exitCode) {
		Assertions.assertEquals(0, exitCode);
	}

	protected <T> T command(ParseResult parseResult) {
		return parseResult.subcommands().get(0).commandSpec().commandLine().getCommand();
	}

	protected int execute(TestInfo info, String filename, IExecutionStrategy... executionStrategies) throws Exception {
		String[] args = args(filename);
		IExecutionStrategy executionStrategy = executionStrategy(info, executionStrategies);
		return AbstractMainCommand.run(new Main(), out, err, args, executionStrategy);
	}

	private IExecutionStrategy executionStrategy(TestInfo info, IExecutionStrategy... executionStrategies) {
		CompositeExecutionStrategy strategy = new CompositeExecutionStrategy();
		strategy.addDelegates(r -> execute(info, r));
		strategy.addDelegates(executionStrategies);
		return strategy;
	}

	private int execute(TestInfo info, ParseResult parseResult) {
		for (ParseResult subParseResult : parseResult.subcommands()) {
			Object command = subParseResult.commandSpec().commandLine().getCommand();
			if (command instanceof WriteOperationCommand) {
				command = subParseResult.commandSpec().parent().commandLine().getCommand();
			}
			if (command instanceof AbstractRiotCommand) {
				AbstractRiotCommand riotCommand = ((AbstractRiotCommand) command);
				riotCommand.getJobArgs().setProgressStyle(ProgressStyle.NONE);
				riotCommand.setName(name(info));
			}
			if (command instanceof AbstractImportCommand) {
				configure(((AbstractImportCommand) command).getRedisClientArgs());
			}
			if (command instanceof AbstractExportCommand) {
				configure(((AbstractExportCommand) command).getRedisClientArgs());
			}
			if (command instanceof ReplicateCommand) {
				ReplicateCommand replicateCommand = (ReplicateCommand) command;
				replicateCommand.setCompareMode(CompareMode.NONE);
				configure(replicateCommand.getSourceRedisClientArgs());
				replicateCommand.getTargetRedisClientArgs().getUriArgs().setUri(getTargetRedisServer().getRedisURI());
				replicateCommand.getTargetRedisClientArgs().setCluster(getTargetRedisServer().isRedisCluster());
				if (replicateCommand.getMode() == ReplicationMode.LIVE
						|| replicateCommand.getMode() == ReplicationMode.LIVEONLY) {
					replicateCommand.setIdleTimeout(getIdleTimeout().toMillis());
					replicateCommand.getSourceRedisReaderArgs()
							.setNotificationQueueCapacity(DEFAULT_NOTIFICATION_QUEUE_CAPACITY);
				}
			}
		}
		return ExitCode.OK;
	}

	private void configure(RedisClientArgs redisClientArgs) {
		redisClientArgs.getUriArgs().setUri(getRedisServer().getRedisURI());
		redisClientArgs.setCluster(getRedisServer().isRedisCluster());
	}

	private static String[] args(String filename) throws Exception {
		try (InputStream inputStream = AbstractMainCommand.class.getResourceAsStream("/" + filename)) {
			String command = IOUtils.toString(inputStream, Charset.defaultCharset());
			if (command.startsWith(PREFIX)) {
				command = command.substring(PREFIX.length());
			}
			return translateCommandline(command);
		}
	}

	private static String[] translateCommandline(String toProcess) throws Exception {
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
