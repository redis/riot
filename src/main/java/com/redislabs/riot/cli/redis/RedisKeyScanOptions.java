package com.redislabs.riot.cli.redis;

import com.redislabs.riot.batch.redis.LettuceConnector;
import com.redislabs.riot.batch.redis.reader.LettuceKeyScanReader;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Data;
import lombok.experimental.Accessors;
import picocli.CommandLine.Option;

@Accessors(fluent = true)
public @Data class RedisKeyScanOptions {

	@Option(names = "--count", description = "Number of elements to return for each scan call", paramLabel = "<int>")
	private Long count;
	@Option(names = "--match", description = "Scan match pattern", paramLabel = "<pattern>")
	private String match;

	public LettuceKeyScanReader reader(
			LettuceConnector<StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>> connector) {
		return new LettuceKeyScanReader(connector).count(count).match(match);
	}

}
