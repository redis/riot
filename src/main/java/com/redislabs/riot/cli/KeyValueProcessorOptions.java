package com.redislabs.riot.cli;

import com.redislabs.riot.redis.reader.FieldExtractor;
import com.redislabs.riot.redis.reader.KeyFieldValueMapProcessor;
import com.redislabs.riot.redis.reader.KeyValueMapProcessor;

import picocli.CommandLine.Option;

public class KeyValueProcessorOptions {

	@Option(names = "--key-regex", description = "Regular expression for key-field extraction", paramLabel = "<regex>")
	private String keyRegex;

	public KeyValueMapProcessor processor() {
		if (keyRegex == null) {
			return new KeyValueMapProcessor();
		}
		return new KeyFieldValueMapProcessor(FieldExtractor.builder().regex(keyRegex).build());
	}
}
