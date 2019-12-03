package com.redislabs.riot.cli.redis;

import lombok.Data;
import lombok.experimental.Accessors;
import picocli.CommandLine.Option;

@Accessors(fluent = true)
public @Data class RedisKeyScanOptions {

	@Option(names = "--count", description = "Number of elements to return for each scan call", paramLabel = "<int>")
	private Long count;
	@Option(names = "--match", description = "Scan match pattern", paramLabel = "<pattern>")
	private String match;

}
