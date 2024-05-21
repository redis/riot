package com.redis.riot.core;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

public interface StepConfiguration {

	<I, O> void configure(SimpleStepBuilder<I, O> step, String name, ItemReader<I> reader, ItemWriter<O> writer);

}
