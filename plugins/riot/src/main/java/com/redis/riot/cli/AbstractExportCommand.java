package com.redis.riot.cli;

import com.redis.riot.core.AbstractExport;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractRiotCommand {

	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private ExportArgs exportArgs = new ExportArgs();

	@ArgGroup(exclusive = false)
	private KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

	@Override
	protected AbstractExport callable() {
		AbstractExport export = exportCallable();
		export.setRedisClientOptions(exportArgs.getRedisClientArgs().redisClientOptions());
		export.setReaderOptions(exportArgs.getRedisReaderArgs().redisReaderOptions());
		export.setProcessorOptions(processorArgs.processorOptions());
		return export;
	}

	protected abstract AbstractExport exportCallable();

	public static class ExportArgs {

		@ArgGroup(exclusive = false)
		private RedisClientArgs redisClientArgs = new RedisClientArgs();

		@ArgGroup(exclusive = false)
		private RedisReaderArgs redisReaderArgs = new RedisReaderArgs();

		public RedisClientArgs getRedisClientArgs() {
			return redisClientArgs;
		}

		public void setRedisClientArgs(RedisClientArgs redisClientArgs) {
			this.redisClientArgs = redisClientArgs;
		}

		public RedisReaderArgs getRedisReaderArgs() {
			return redisReaderArgs;
		}

		public void setRedisReaderArgs(RedisReaderArgs redisReaderArgs) {
			this.redisReaderArgs = redisReaderArgs;
		}

	}

	public KeyValueProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(KeyValueProcessorArgs args) {
		this.processorArgs = args;
	}

	public ExportArgs getExportArgs() {
		return exportArgs;
	}

	public void setExportArgs(ExportArgs exportArgs) {
		this.exportArgs = exportArgs;
	}

}
