package com.redis.riot;

import java.util.Optional;

import picocli.CommandLine.Option;

public class KeyValueProcessorOptions {

	@Option(names = "--key-process", description = "SpEL expression to transform each key", paramLabel = "<exp>")
	private Optional<String> keyProcessor = Optional.empty();
	@Option(names = "--ttl-process", description = "SpEL expression to transform each key TTL", paramLabel = "<exp>")
	private Optional<String> ttlProcessor = Optional.empty();

	public Optional<String> getKeyProcessor() {
		return keyProcessor;
	}

	public Optional<String> getTtlProcessor() {
		return ttlProcessor;
	}

}
