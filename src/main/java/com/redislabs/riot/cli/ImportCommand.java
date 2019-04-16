package com.redislabs.riot.cli;

import java.net.UnknownHostException;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.stereotype.Component;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.riot.redis.RedisConfig;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Component
@Command(name = "import", description = "Import data into Redis", subcommands = { DelimitedImportSubCommand.class,
		FixedLengthImportSubCommand.class, JsonImportSubCommand.class,
		DatabaseImportSubCommand.class }, sortOptions = false)
public class ImportCommand extends HelpAwareCommand {

	@Mixin
	private RedisConnectionOptions redis;

	public GenericObjectPool<StatefulRediSearchConnection<String, String>> getPool() throws UnknownHostException {
		RedisConfig redisConfig = new RedisConfig();
		RediSearchClient client = redisConfig.client(getHostname(), redis.getPort(), redis.getPassword(),
				redis.getTimeout());
		return new RedisConfig().pool(client, redis.getPool());
	}

	private String getHostname() throws UnknownHostException {
		if (redis.getHost() == null) {
			return RedisConnectionOptions.DEFAULT_HOST;
		}
		return redis.getHost().getHostName();
	}
}
