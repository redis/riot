package com.redis.riot.core;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.batch.item.ItemProcessor;

import com.redis.riot.core.function.KeyValueMapProcessor;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.spring.batch.common.KeyValue;

public class KeyValueMapProcessorOptions {

	private String keyRegex;

	public String getKeyRegex() {
		return keyRegex;
	}

	public void setKeyRegex(String pattern) {
		this.keyRegex = pattern;
	}

	public ItemProcessor<KeyValue<String, Object>, Map<String, Object>> processor() {
		KeyValueMapProcessor processor = new KeyValueMapProcessor();
		if (keyRegex != null) {
			processor.setKey(new RegexNamedGroupFunction(Pattern.compile(keyRegex)));
		}
		return processor;
	}
}
