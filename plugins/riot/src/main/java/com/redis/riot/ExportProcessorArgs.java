package com.redis.riot;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.EvaluationContext;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.function.KeyValueMap;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.Option;

public class ExportProcessorArgs extends ProcessorArgs {

	@Option(names = "--key-regex", description = "Regex for key-field extraction, e.g. '\\w+:(?<id>.+)' extracts an id field from the key", paramLabel = "<rex>")
	private Pattern keyRegex;

	public ItemProcessor<KeyValue<String, Object>, Map<String, Object>> mapProcessor(EvaluationContext context) {
		KeyValueMap mapFunction = new KeyValueMap();
		if (keyRegex != null) {
			mapFunction.setKey(new RegexNamedGroupFunction(keyRegex));
		}
		return RiotUtils.processor(keyValueProcessor(context), new FunctionItemProcessor<>(mapFunction));
	}

	public Pattern getKeyRegex() {
		return keyRegex;
	}

	public void setKeyRegex(Pattern regex) {
		this.keyRegex = regex;
	}

	@Override
	public String toString() {
		return "ExportProcessorArgs [keyRegex=" + keyRegex + ", " + super.toString() + "]";
	}

}
