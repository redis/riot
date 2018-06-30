package com.redislabs.recharge.dummy;

import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.batch.StepProvider;

@Configuration
public class DummyStep implements StepProvider {

	@Override
	public DummyItemReader getReader() throws Exception {
		return new DummyItemReader();
	}

	@Override
	public DummyItemProcessor getProcessor() {
		return new DummyItemProcessor();
	}

}
