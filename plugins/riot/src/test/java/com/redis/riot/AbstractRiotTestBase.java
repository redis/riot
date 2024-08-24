package com.redis.riot;

import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.function.Consumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;

import com.redis.riot.Replicate.CompareMode;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.ProgressStyle;
import com.redis.riot.operation.OperationCommand;
import com.redis.spring.batch.test.AbstractTargetTestBase;

import io.micrometer.core.instrument.util.IOUtils;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;

abstract class AbstractRiotTestBase extends AbstractTargetTestBase {

	public static final long DEFAULT_IDLE_TIMEOUT_SECONDS = 1;
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 100000;
	private static final String PREFIX = "riot ";

	private PrintWriter out = new PrintWriter(System.out);
	private PrintWriter err = new PrintWriter(System.err);

	static {
		System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
		System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "info");
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + ReplicateWriteLogger.class.getName(), "error");
	}

	protected static void assertExecutionSuccessful(int exitCode) {
		Assertions.assertEquals(0, exitCode);
	}

	protected <T> T command(ParseResult parseResult) {
		return parseResult.subcommands().get(0).commandSpec().commandLine().getCommand();
	}

	@SuppressWarnings("unchecked")
	protected int execute(TestInfo info, String filename) throws Exception {
		return doExecute(info, filename);
	}

	@SuppressWarnings("unchecked")
	protected int execute(TestInfo info, String filename, Consumer<ParseResult> config) throws Exception {
		return doExecute(info, filename, config);
	}

	@SuppressWarnings("unchecked")
	private int doExecute(TestInfo info, String filename, Consumer<ParseResult>... configs) throws Exception {
		String[] args = args(filename);
		Main main = new Main();
		main.setOut(out);
		main.setErr(err);
		CommandLine commandLine = new CommandLine(main);
		commandLine.setOut(out);
		commandLine.setErr(err);
		ExecutionStrategy executionStrategy = new ExecutionStrategy();
		executionStrategy.add(r -> configure(info, r));
		executionStrategy.add(configs);
		commandLine.setExecutionStrategy(executionStrategy);
		return Main.run(commandLine, args);
	}

	private class ExecutionStrategy implements IExecutionStrategy {

		private final IExecutionStrategy delegate = Main::executionStrategy;

		private final List<Consumer<ParseResult>> configs = new ArrayList<>();

		@SuppressWarnings("unchecked")
		public void add(Consumer<ParseResult>... configs) {
			this.configs.addAll(Arrays.asList(configs));
		}

		@Override
		public int execute(ParseResult parseResult) throws ExecutionException, ParameterException {
			configs.forEach(c -> c.accept(parseResult));
			return delegate.execute(parseResult);
		}

	}

	private void configure(TestInfo info, ParseResult parseResult) {
		for (ParseResult subParseResult : parseResult.subcommands()) {
			Object command = subParseResult.commandSpec().commandLine().getCommand();
			if (command instanceof OperationCommand) {
				command = subParseResult.commandSpec().parent().commandLine().getCommand();
			}
			if (command instanceof AbstractJobCommand) {
				AbstractJobCommand jobCommand = ((AbstractJobCommand) command);
				jobCommand.getJobArgs().getProgressArgs().setStyle(ProgressStyle.NONE);
				jobCommand.setJobName(name(info));
			}
			if (command instanceof AbstractRedisCommand) {
				AbstractRedisCommand redisCommand = (AbstractRedisCommand) command;
				redisCommand.getRedisArgs().setUri(redisURI);
				redisCommand.getRedisArgs().setCluster(getRedisServer().isRedisCluster());
			}
			if (command instanceof AbstractExportCommand) {
				AbstractExportCommand exportCommand = (AbstractExportCommand) command;
				configure(exportCommand.getRedisReaderArgs());
			}
			if (command instanceof AbstractRedisToRedisCommand) {
				AbstractRedisToRedisCommand targetCommand = (AbstractRedisToRedisCommand) command;
				configure(targetCommand.getSourceRedisReaderArgs());
				targetCommand.setSourceRedisURI(redisURI);
				targetCommand.getSourceRedisClientArgs().setCluster(getRedisServer().isRedisCluster());
				targetCommand.setTargetRedisURI(targetRedisURI);
				targetCommand.getTargetRedisClientArgs().setCluster(getTargetRedisServer().isRedisCluster());
			}
			if (command instanceof Replicate) {
				Replicate replicateCommand = (Replicate) command;
				replicateCommand.setCompareMode(CompareMode.NONE);
			}
		}
	}

	private void configure(RedisReaderArgs redisReaderArgs) {
		redisReaderArgs.setIdleTimeout(DEFAULT_IDLE_TIMEOUT_SECONDS);
		redisReaderArgs.setNotificationQueueCapacity(DEFAULT_NOTIFICATION_QUEUE_CAPACITY);
	}

	private static String[] args(String filename) throws Exception {
		try (InputStream inputStream = Main.class.getResourceAsStream("/" + filename)) {
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
