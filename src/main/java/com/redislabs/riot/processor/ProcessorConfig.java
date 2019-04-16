package com.redislabs.riot.processor;

import org.springframework.context.annotation.Bean;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

public class ProcessorConfig {

	@Bean
	public SpelProcessor processor(ProcessorProperties props, StatefulRediSearchConnection<String, String> connection) {
		SpelProcessor processor = new SpelProcessor();
		processor.setConnection(connection);
		processor.setSourceExpression(props.getSource());
		processor.setMergeExpression(props.getMerge());
		processor.setFields(props.getFields());
		return processor;
	}

}