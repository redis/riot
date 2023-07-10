package com.redis.riot.cli.common;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.Builder;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractStructImportCommand extends AbstractImportCommand {

	@ArgGroup(exclusive = false, heading = "Data structure options%n")
	protected RedisStructOptions structOptions = new RedisStructOptions();

	protected RedisItemWriter<String, String> writer(CommandContext context) {
		Builder<String, String> writer = RedisItemWriter.client(context.getRedisClient());
		writer.structOptions(structOptions.structOptions());
		configure(writer);
		return writer.struct();
	}

}
