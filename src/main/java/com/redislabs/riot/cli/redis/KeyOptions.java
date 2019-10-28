package com.redislabs.riot.cli.redis;

import com.redislabs.riot.batch.redis.RedisConverter;

import lombok.Setter;
import picocli.CommandLine.Option;

public class KeyOptions {

	@Setter
	@Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String separator = ":";
	@Setter
	@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
	private String keyspace;
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];

	public void setKeys(String... keys) {
		this.keys = keys;
	}

	public RedisConverter converter() {
		return new RedisConverter(separator, keyspace, keys);
	}

}
