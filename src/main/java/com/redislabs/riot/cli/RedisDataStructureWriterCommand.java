package com.redislabs.riot.cli;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.riot.cli.redis.ExpireWriterCommand;
import com.redislabs.riot.cli.redis.GeoWriterCommand;
import com.redislabs.riot.cli.redis.HashWriterCommand;
import com.redislabs.riot.cli.redis.ListWriterCommand;
import com.redislabs.riot.cli.redis.LuaWriterCommand;
import com.redislabs.riot.cli.redis.SetWriterCommand;
import com.redislabs.riot.cli.redis.StreamWriterCommand;
import com.redislabs.riot.cli.redis.StringWriterCommand;
import com.redislabs.riot.cli.redis.ZSetWriterCommand;

import io.lettuce.core.api.StatefulRedisConnection;
import picocli.CommandLine.Command;

@Command(name = "redis", description = "Redis", subcommands = { GeoWriterCommand.class, HashWriterCommand.class,
		ListWriterCommand.class, SetWriterCommand.class, StreamWriterCommand.class, StringWriterCommand.class,
		ZSetWriterCommand.class, LuaWriterCommand.class,
		ExpireWriterCommand.class }, synopsisSubcommandLabel = "[TYPE]", commandListHeading = "Types:%n")
public class RedisDataStructureWriterCommand
		extends AbstractRedisWriterCommand<StatefulRedisConnection<String, String>> {

	@Override
	protected GenericObjectPool<StatefulRedisConnection<String, String>> lettucePool(RedisConnectionOptions redis) {
		return redis.lettucePool();
	}

}
