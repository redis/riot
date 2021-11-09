package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractKeyCommand extends AbstractRedisCommand<Map<String, Object>> {

	@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
	private String keyspace = "";
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
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

	protected Converter<Map<String, Object>, String> key() {
		return idMaker(keyspace, keys);
	}

}
