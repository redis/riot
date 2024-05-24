package com.redis.riot;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class KeyValueMapProcessorArgs {

	@Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rgx>")
	private Pattern keyRegex;

	@ArgGroup(exclusive = false)
	private KeyValueProcessorArgs keyValueProcessorArgs = new KeyValueProcessorArgs();

	public ItemProcessor<KeyValue<String, Object>, Map<String, Object>> processor(EvaluationContext evaluationContext) {
		return RiotUtils.processor(keyValueProcessorArgs.processor(evaluationContext), mapProcessor());
	}

	private KeyValueMapProcessor mapProcessor() {
		KeyValueMapProcessor processor = new KeyValueMapProcessor();
		if (keyRegex != null) {
			processor.setKey(new RegexNamedGroupFunction(keyRegex));
		}
		return processor;
	}

	public KeyValueProcessorArgs getKeyValueProcessorArgs() {
		return keyValueProcessorArgs;
	}

	public void setKeyValueProcessorArgs(KeyValueProcessorArgs args) {
		this.keyValueProcessorArgs = args;
	}

	public Pattern getKeyRegex() {
		return keyRegex;
	}

	public void setKeyRegex(Pattern regex) {
		this.keyRegex = regex;
	}

	@Override
	public String toString() {
		return "KeyValueMapProcessorArgs [keyRegex=" + keyRegex + ", keyValueProcessorArgs=" + keyValueProcessorArgs
				+ "]";
	}

}
