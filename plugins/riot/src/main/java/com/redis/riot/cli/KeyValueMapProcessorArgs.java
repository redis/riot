package com.redis.riot.cli;

import java.util.regex.Pattern;

import com.redis.riot.core.KeyValueMapProcessorOptions;

import picocli.CommandLine.Option;

public class KeyValueMapProcessorArgs {

	@Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rgx>")
	private Pattern keyRegex;

	public KeyValueMapProcessorOptions keyValueMapProcessorOptions() {
		KeyValueMapProcessorOptions options = new KeyValueMapProcessorOptions();
		options.setKeyRegex(keyRegex.pattern());
		return options;
	}
}
