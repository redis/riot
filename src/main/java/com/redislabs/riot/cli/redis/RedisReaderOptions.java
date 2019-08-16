package com.redislabs.riot.cli.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redislabs.riot.redis.RedisReader;

import picocli.CommandLine.Option;
import redis.clients.jedis.Jedis;

public class RedisReaderOptions {

	private final Logger log = LoggerFactory.getLogger(RedisReaderOptions.class);

	@Option(names = "--count", description = "Number of elements to return for each scan call")
	private Integer count;
	@Option(names = "--key-separator", description = "Redis key separator (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String separator = ":";
	@Option(names = "--keyspace", description = "Redis keyspace prefix", paramLabel = "<string>")
	private String keyspace;
	@Option(names = "--keys", arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];

	public RedisReader reader(Jedis jedis) {
		String scanPattern = scanPattern();
		log.info("Creating Redis reader with match={} and count={}", scanPattern, count);
		RedisReader reader = new RedisReader(jedis);
		reader.setCount(count);
		reader.setMatch(scanPattern);
		reader.setKeys(keys);
		reader.setKeyspace(keyspace);
		reader.setSeparator(separator);
		return reader;
	}

	public String scanPattern() {
		if (keyspace == null) {
			return null;
		}
		return keyspace + separator + "*";
	}
}
