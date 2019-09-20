package com.redislabs.riot.cli;

import com.redislabs.riot.cli.redis.RediSearchCommandOptions;
import com.redislabs.riot.cli.redis.RedisCommandOptions;
import com.redislabs.riot.cli.redis.RedisKeyOptions;
import com.redislabs.riot.redis.writer.AbstractRedisMapWriter;
import com.redislabs.riot.redis.writer.RedisMapWriter;

import picocli.CommandLine.ArgGroup;

public class RedisWriterOptions {

	@ArgGroup(exclusive = false, heading = "Redis key options%n")
	private RedisKeyOptions keyOptions = new RedisKeyOptions();
	@ArgGroup(exclusive = false, heading = "Redis command options%n")
	private RedisCommandOptions redis = new RedisCommandOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch command options%n")
	private RediSearchCommandOptions search = new RediSearchCommandOptions();

	public RedisMapWriter writer() {
		RedisMapWriter writer = search.isSet() ? search.writer() : redis.writer();
		if (writer instanceof AbstractRedisMapWriter) {
			((AbstractRedisMapWriter) writer).setConverter(keyOptions.converter());
		}
		return writer;
	}

}
