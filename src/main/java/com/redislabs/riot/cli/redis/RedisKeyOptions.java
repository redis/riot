package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.RedisConverter;

import picocli.CommandLine.Option;

public class RedisKeyOptions {

	@Option(names = "--key-separator", description = "Redis key separator (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String separator = ":";
	@Option(names = { "-p", "--keyspace" }, description = "Redis keyspace prefix", paramLabel = "<string>")
	private String keyspace;
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];

	public RedisConverter converter() {
		return new RedisConverter(separator, keyspace, keys);
	}

}
