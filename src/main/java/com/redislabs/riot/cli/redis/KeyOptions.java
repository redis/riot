package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.RedisConverter;

import picocli.CommandLine.Option;

public class KeyOptions {

	@Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String separator = ":";
	@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
	private String keyspace;
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];

	public RedisConverter converter() {
		return new RedisConverter(separator, keyspace, keys);
	}

}
