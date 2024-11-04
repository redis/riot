package com.redis.riot;

import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.MainCommand;
import com.redis.riot.core.ProgressStyle;
import com.redis.riot.operation.OperationCommand;
import com.redis.riot.test.AbstractRiotTestBase;

import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParseResult;

abstract class AbstractRiotApplicationTestBase extends AbstractRiotTestBase {

	private static final String PREFIX = "riot ";

	static {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + ReplicateWriteLogger.class.getName(), "error");
	}

	@Override
	protected String getMainCommandPrefix() {
		return PREFIX;
	}

	@Override
	protected MainCommand mainCommand(TestInfo info, IExecutionStrategy... executionStrategies) {
		return new TestRiot(info, executionStrategies);
	}

	private class TestRiot extends Riot {

		private final TestInfo info;
		private final IExecutionStrategy[] configs;

		public TestRiot(TestInfo info, IExecutionStrategy... configs) {
			this.info = info;
			this.configs = configs;
		}

		private void configure(RedisArgs redisArgs) {
			redisArgs.setUri(redisURI);
			redisArgs.setCluster(getRedisServer().isRedisCluster());
		}

		private void configure(RedisReaderArgs redisReaderArgs) {
			redisReaderArgs.setIdleTimeout(DEFAULT_IDLE_TIMEOUT_SECONDS);
			redisReaderArgs.setEventQueueCapacity(DEFAULT_EVENT_QUEUE_CAPACITY);
		}

		@Override
		protected int execute(ParseResult parseResult, IExecutionStrategy defaultStrategy) {
			configure(parseResult);
			for (IExecutionStrategy config : configs) {
				config.execute(parseResult);
			}
			return super.execute(parseResult, defaultStrategy);
		}

		private void configure(ParseResult parseResult) {
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
					configure(((AbstractRedisCommand) command).getRedisArgs());
				}
				if (command instanceof AbstractRedisExportCommand) {
					configure(((AbstractRedisExportCommand) command).getRedisArgs());
				}
				if (command instanceof AbstractRedisImportCommand) {
					configure(((AbstractRedisImportCommand) command).getRedisArgs());
				}
				if (command instanceof AbstractExportCommand) {
					configure(((AbstractExportCommand) command).getSourceRedisReaderArgs());
				}
				if (command instanceof AbstractRedisTargetExportCommand) {
					AbstractRedisTargetExportCommand targetCommand = (AbstractRedisTargetExportCommand) command;
					configure(targetCommand.getSourceRedisReaderArgs());
					targetCommand.setSourceRedisUri(redisURI);
					targetCommand.getSourceRedisArgs().setCluster(getRedisServer().isRedisCluster());
					targetCommand.setTargetRedisUri(targetRedisURI);
					targetCommand.getTargetRedisArgs().setCluster(getTargetRedisServer().isRedisCluster());
				}
				if (command instanceof Replicate) {
					Replicate replicateCommand = (Replicate) command;
					replicateCommand.setCompareMode(CompareMode.NONE);
				}
			}
		}
	}

}
