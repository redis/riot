package com.redis.riot;

import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand {

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions options = new RedisReaderOptions();

	protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(String name, String taskName,
			ItemWriter<O> writer) throws Exception {
		return step(name, taskName, null, writer);
	}

	protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(String name, String taskName,
			ItemProcessor<DataStructure<String>, O> processor, ItemWriter<O> writer) throws Exception {
		RiotStepBuilder<DataStructure<String>, O> step = riotStep(name, taskName);
		return step.reader(reader()).processor(processor).writer(writer).build();
	}

	private final RedisItemReader<String, DataStructure<String>> reader() throws Exception {
		return options.configureScanReader(configureJobRepository(reader(getRedisOptions()).dataStructureIntrospect())).build();
	}

	@Override
	protected <S, T> RiotStepBuilder<S, T> riotStep(String name, String taskName) throws Exception {
		RiotStepBuilder<S, T> riotStepBuilder = super.riotStep(name, taskName);
		riotStepBuilder.initialMax(options.initialMaxSupplier(estimator()));
		return riotStepBuilder;
	}

}
