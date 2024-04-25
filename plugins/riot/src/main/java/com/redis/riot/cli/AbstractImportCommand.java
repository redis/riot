package com.redis.riot.cli;

import com.redis.riot.core.AbstractImport;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractImportCommand extends AbstractRiotCommand {

	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private ImportArgs importArgs = new ImportArgs();

	@Override
	protected AbstractImport callable() {
		AbstractImport callable = importCallable();
		callable.setRedisClientOptions(importArgs.getRedisClientArgs().redisClientOptions());
		callable.setWriterOptions(importArgs.getRedisWriterArgs().writerOptions());
		return callable;
	}

	protected abstract AbstractImport importCallable();

	public static class ImportArgs {

		@ArgGroup(exclusive = false)
		private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

		@ArgGroup(exclusive = false)
		private RedisClientArgs redisClientArgs = new RedisClientArgs();

		public RedisWriterArgs getRedisWriterArgs() {
			return redisWriterArgs;
		}

		public void setRedisWriterArgs(RedisWriterArgs args) {
			this.redisWriterArgs = args;
		}

		public RedisClientArgs getRedisClientArgs() {
			return redisClientArgs;
		}

		public void setRedisClientArgs(RedisClientArgs args) {
			this.redisClientArgs = args;
		}

	}

	public ImportArgs getImportArgs() {
		return importArgs;
	}

	public void setImportArgs(ImportArgs args) {
		this.importArgs = args;
	}

}
