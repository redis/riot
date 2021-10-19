package com.redis.riot;

import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.DataStructure;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand {

	@CommandLine.ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderOptions options = new RedisReaderOptions();

	protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(StepBuilder stepBuilder,
			String taskName, ItemWriter<O> writer) throws Exception {
		return step(stepBuilder, taskName, null, writer);
	}

	protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(StepBuilder stepBuilder,
			String taskName, ItemProcessor<DataStructure<String>, O> processor, ItemWriter<O> writer) throws Exception {
		RiotStepBuilder<DataStructure<String>, O> step = riotStep(stepBuilder, taskName);
		return step.reader(reader()).processor(processor).writer(writer).build();
	}

	protected final RedisItemReader<String, DataStructure<String>> reader() {
		AbstractRedisClient client = getRedisOptions().client();
		return options.configure(RedisItemReader.dataStructure(client)).build();
	}

	@Override
	protected <S, T> RiotStepBuilder<S, T> riotStep(StepBuilder stepBuilder, String taskName) {
		RiotStepBuilder<S, T> riotStepBuilder = super.riotStep(stepBuilder, taskName);
		riotStepBuilder.initialMax(options.initialMaxSupplier(getRedisOptions()));
		return riotStepBuilder;
	}

}
