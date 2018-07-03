package com.redislabs.recharge.dummy;

import org.springframework.context.annotation.Configuration;

@Configuration
public class DummyStep {

	public DummyItemReader reader() {
		return new DummyItemReader();
	}

	public DummyItemProcessor processor() {
		return new DummyItemProcessor();
	}

	public DummyItemWriter writer() {
		return new DummyItemWriter();
	}

}
