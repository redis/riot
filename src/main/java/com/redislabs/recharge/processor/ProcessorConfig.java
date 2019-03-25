package com.redislabs.recharge.processor;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.StatefulRediSearchConnection;

@Configuration
@EnableConfigurationProperties(ProcessorProperties.class)
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