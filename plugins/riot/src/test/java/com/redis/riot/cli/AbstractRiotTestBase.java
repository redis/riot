package com.redis.riot.cli;

import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;

import com.redis.riot.cli.AbstractImportCommand.OperationCommand;
import com.redis.riot.cli.AbstractJobCommand.ProgressStyle;
import com.redis.riot.core.ReplicationMode;
import com.redis.spring.batch.test.AbstractTargetTestBase;
import com.redis.testcontainers.RedisServer;

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
	}

	protected static void assertExecutionSuccessful(int exitCode) {
		Assertions.assertEquals(0, exitCode);
	}

	protected <T> T command(ParseResult parseResult) {
		return parseResult.subcommands().get(0).commandSpec().commandLine().getCommand();
	}

	protected int execute(TestInfo info, String filename, IExecutionStrategy... executionStrategies) throws Exception {
		String[] args = args(filename);
		IExecutionStrategy executionStrategy = executionStrategy(info, filename, executionStrategies);
		return Main.run(out, err, args, executionStrategy);
	}

	private IExecutionStrategy executionStrategy(TestInfo info, String name,
			IExecutionStrategy... executionStrategies) {
		CompositeExecutionStrategy strategy = new CompositeExecutionStrategy();
		strategy.addDelegates(r -> execute(info, name, r));
		strategy.addDelegates(executionStrategies);
		return strategy;
	}

	private int execute(TestInfo info, String name, ParseResult parseResult) {
		RedisServer server = getRedisServer();
		Main main = (Main) parseResult.commandSpec().commandLine().getCommand();
		main.redisArgs.uri = server.getRedisURI();
		main.redisArgs.cluster = server.isRedisCluster();
		for (ParseResult subParseResult : parseResult.subcommands()) {
			Object command = subParseResult.commandSpec().commandLine().getCommand();
			if (command instanceof OperationCommand) {
				command = subParseResult.commandSpec().parent().commandLine().getCommand();
			}
			if (command instanceof AbstractJobCommand) {
				AbstractJobCommand jobCommand = ((AbstractJobCommand) command);
				jobCommand.setProgressStyle(ProgressStyle.NONE);
				jobCommand.setName(name(testInfo(info, name)));
			}
			if (command instanceof ReplicateCommand) {
				ReplicateCommand replicationCommand = (ReplicateCommand) command;
				replicationCommand.targetRedisArgs.uri = getTargetRedisServer().getRedisURI();
				if (replicationCommand.mode == ReplicationMode.LIVE
						|| replicationCommand.mode == ReplicationMode.LIVEONLY) {
					replicationCommand.readerArgs.setIdleTimeout(getIdleTimeout().toMillis());
					replicationCommand.readerArgs.setNotificationQueueCapacity(DEFAULT_NOTIFICATION_QUEUE_CAPACITY);
				}
			}
		}
		return ExitCode.OK;
	}

	private static String[] args(String filename) throws Exception {
		try (InputStream inputStream = Main.class.getResourceAsStream("/" + filename)) {
			String command = IOUtils.toString(inputStream, Charset.defaultCharset());
			if (command.startsWith(PREFIX)) {
				command = command.substring(PREFIX.length());
			}
			return CommandLineUtils.translateCommandline(command);
		}
	}

}
