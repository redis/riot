package com.redislabs.riot.cli;

import java.text.MessageFormat;
import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.redis.RediSearchReaderOptions;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;
import com.redislabs.riot.cli.redis.RedisReaderOptions;

import picocli.CommandLine.ArgGroup;

public abstract class ExportCommand extends TransferCommand {

	@ArgGroup(exclusive = false, heading = "Redis connection options%n")
	private RedisConnectionOptions redis = new RedisConnectionOptions();
	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderOptions redisReader = new RedisReaderOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch reader options%n")
	private RediSearchReaderOptions searchReader = new RediSearchReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() throws Exception {
		if (searchReader.isSet()) {
			return searchReader.reader(redis.clientResources(), redis.redisUri());
		}
		return redisReader.reader(redis.jedis());
	}

	@Override
	protected String sourceDescription() {
		if (searchReader.isSet()) {
			return MessageFormat.format("RediSearch index %s query %s", searchReader.getIndex(),
					searchReader.getQuery());
		}
		return MessageFormat.format("Redis %s", redisReader.scanPattern());
	}

}
