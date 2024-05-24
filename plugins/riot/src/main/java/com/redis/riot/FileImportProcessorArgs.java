package com.redis.riot;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class FileImportProcessorArgs extends ImportProcessorArgs {

	@ArgGroup(exclusive = false)
	private KeyValueProcessorArgs keyValueProcessorArgs = new KeyValueProcessorArgs();

	@Option(arity = "1..*", names = "--regex", description = "Regular expressions used to extract fields from in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
	private Map<String, Pattern> regexes = new LinkedHashMap<>();

	public KeyValueProcessorArgs getKeyValueProcessorArgs() {
		return keyValueProcessorArgs;
	}

	public void setKeyValueProcessorArgs(KeyValueProcessorArgs keyValueProcessorArgs) {
		this.keyValueProcessorArgs = keyValueProcessorArgs;
	}

	public Map<String, Pattern> getRegexes() {
		return regexes;
	}

	public void setRegexes(Map<String, Pattern> regexes) {
		this.regexes = regexes;
	}

}
