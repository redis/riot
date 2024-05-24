package com.redis.riot;

import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.Vector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.AbstractMain;
import com.redis.riot.core.CompositeExecutionStrategy;
import com.redis.riot.core.ProgressStyle;
import com.redis.riot.operation.OperationCommand;
import com.redis.spring.batch.test.AbstractTargetTestBase;

import io.lettuce.core.RedisURI;
import io.micrometer.core.instrument.util.IOUtils;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;

abstract class AbstractRiotTestBase extends AbstractTargetTestBase {

	public static final long DEFAULT_IDLE_TIMEOUT_SECONDS = 1;
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 100000;
	private static final String PREFIX = "riot ";

	private PrintWriter out = new PrintWriter(System.out);
	private PrintWriter err = new PrintWriter(System.err);

	static {
		System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + LoggingKeyValueWriteListener.class.getName(), "error");
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
		return AbstractMain.run(new Main(), out, err, args, executionStrategy);
	}

	private IExecutionStrategy executionStrategy(TestInfo info, IExecutionStrategy... executionStrategies) {
		ArrayList<IExecutionStrategy> strategies = new ArrayList<>();
		strategies.add(r -> execute(info, r));
		strategies.addAll(Arrays.asList(executionStrategies));
		return new CompositeExecutionStrategy(strategies);
	}

	private int execute(TestInfo info, ParseResult parseResult) {
		for (ParseResult subParseResult : parseResult.subcommands()) {
			Object command = subParseResult.commandSpec().commandLine().getCommand();
			if (command instanceof OperationCommand) {
				command = subParseResult.commandSpec().parent().commandLine().getCommand();
			}
			if (command instanceof AbstractJobCommand) {
				AbstractJobCommand riotCommand = ((AbstractJobCommand) command);
				riotCommand.getJobArgs().getProgressArgs().setStyle(ProgressStyle.NONE);
				riotCommand.getJobArgs().setName(name(info));
			}
			if (command instanceof AbstractRedisCommand) {
				AbstractRedisCommand redisCommand = (AbstractRedisCommand) command;
				redisCommand.getRedisArgs().setUri(RedisURI.create(getRedisServer().getRedisURI()));
			}
			if (command instanceof AbstractExport) {
				AbstractExport exportCommand = (AbstractExport) command;
				configure(exportCommand.getRedisArgs());
				configure(exportCommand.getRedisReaderArgs());
			}
			if (command instanceof AbstractTargetExport) {
				AbstractTargetExport redisExportCommand = (AbstractTargetExport) command;
				redisExportCommand.getTargetRedisArgs().setUri(targetRedisURI);
				redisExportCommand.getTargetRedisArgs().setCluster(getTargetRedisServer().isRedisCluster());
			}
			if (command instanceof Replicate) {
				Replicate replicateCommand = (Replicate) command;
				replicateCommand.getCompareArgs().setMode(CompareMode.NONE);
			}
		}
		return ExitCode.OK;
	}

	private void configure(RedisReaderArgs redisReaderArgs) {
		redisReaderArgs.setIdleTimeout(DEFAULT_IDLE_TIMEOUT_SECONDS);
		redisReaderArgs.setNotificationQueueCapacity(DEFAULT_NOTIFICATION_QUEUE_CAPACITY);
	}

	private void configure(RedisArgs redisArgs) {
		redisArgs.setUri(redisURI);
		redisArgs.setCluster(getRedisServer().isRedisCluster());

	}

	private static String[] args(String filename) throws Exception {
		try (InputStream inputStream = AbstractMain.class.getResourceAsStream("/" + filename)) {
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
