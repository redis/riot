package com.redislabs.recharge.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
public class ProcessorConfiguration {

	private String source;
	private String merge;
	private Map<String, String> fields = new LinkedHashMap<>();

	public SpelItemProcessor processor() {
		SpelItemProcessor processor = new SpelItemProcessor();
		processor.setSourceExpression(source);
		processor.setMergeExpression(merge);
		processor.setFields(fields);
		return processor;
	}

}