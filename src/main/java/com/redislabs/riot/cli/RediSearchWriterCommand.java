package com.redislabs.riot.cli;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.riot.cli.redis.SearchWriterCommand;
import com.redislabs.riot.cli.redis.SuggestWriterCommand;

import picocli.CommandLine.Command;

@Command(name = "redisearch", description = "RediSearch", subcommands = { SearchWriterCommand.class,
		SuggestWriterCommand.class }, synopsisSubcommandLabel = "[TYPE]", commandListHeading = "Types:%n")
public class RediSearchWriterCommand extends AbstractRedisWriterCommand<StatefulRediSearchConnection<String, String>> {

	@Override
	protected GenericObjectPool<StatefulRediSearchConnection<String, String>> lettucePool(
			RedisConnectionOptions redis) {
		return redis.lettusearchPool();
	}

}
