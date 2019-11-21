package com.redislabs.riot.cli.redis;

import java.util.ArrayList;
import java.util.List;

import com.redislabs.riot.batch.redis.JedisMapReader;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Option;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

@Slf4j
public @Data class RedisReaderOptions {

	@Option(names = "--count", description = "Number of elements to return for each scan call", paramLabel = "<int>")
	private Integer count;
	@Option(names = "--scan-key-sep", description = "Redis key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String separator = ":";
	@Option(names = { "--scan-keyspace" }, description = "Redis keyspace prefix", paramLabel = "<str>")
	private String keyspace;
	@Option(names = { "--scan-keys" }, arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private List<String> keys = new ArrayList<>();

	public JedisMapReader reader(Pool<Jedis> jedisPool) {
		String scanPattern = scanPattern();
		log.debug("Creating Redis reader with match={} and count={}", scanPattern, count);
		JedisMapReader reader = new JedisMapReader(jedisPool);
		reader.setCount(count);
		reader.setMatch(scanPattern);
		reader.setKeys(keys.toArray(new String[keys.size()]));
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
