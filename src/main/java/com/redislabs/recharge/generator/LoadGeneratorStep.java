package com.redislabs.recharge.generator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LoadGeneratorStep {

	@Autowired
	private RecordItemReader reader;

	@Autowired
	private RecordItemWriter writer;

	public RecordItemReader recordReader() {
		return reader;
	}

	public RecordItemWriter recordWriter() {
		return writer;
	}

}