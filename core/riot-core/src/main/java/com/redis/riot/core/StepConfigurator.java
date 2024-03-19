package com.redis.riot.core;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

public interface StepConfigurator {

	void configure(SimpleStepBuilder<?, ?> step, String stepName, ItemReader<?> reader, ItemWriter<?> writer);

}
