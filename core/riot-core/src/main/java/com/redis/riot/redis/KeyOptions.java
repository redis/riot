package com.redis.riot.redis;

import picocli.CommandLine.Option;

public class KeyOptions {

	@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix.", paramLabel = "<str>")
	private String keyspace = "";
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields.", paramLabel = "<fields>")
	private String[] keys;

	public String[] getKeys() {
		return keys;
	}

	public void setKeys(String[] keys) {
		this.keys = keys;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

}
