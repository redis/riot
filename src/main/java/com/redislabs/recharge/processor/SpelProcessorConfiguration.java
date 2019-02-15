package com.redislabs.recharge.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
public class SpelProcessorConfiguration {
	private String source;
	private String merge;
	private Map<String, String> fields = new LinkedHashMap<>();

	public SpelProcessor processor() {
		SpelProcessor processor = new SpelProcessor();
		processor.setSourceExpression(source);
		processor.setMergeExpression(merge);
		processor.setFields(fields);
		return processor;
	}

}