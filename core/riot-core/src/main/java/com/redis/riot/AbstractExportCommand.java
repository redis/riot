package com.redis.riot;

import java.util.Optional;

import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.ScanRedisItemReaderBuilder;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand {

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions options = new RedisReaderOptions();

	protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(String name, String taskName,
			Optional<ItemProcessor<DataStructure<String>, O>> processor, ItemWriter<O> writer) throws Exception {
		RiotStep.Builder<DataStructure<String>, O> step = RiotStep.builder();
		step.name(name).taskName(taskName);
		step.initialMax(options.initialMaxSupplier(estimator()));
		return step(step.reader(reader()).processor(processor).writer(writer).build());
	}

	private final RedisItemReader<String, DataStructure<String>> reader() throws Exception {
		ScanRedisItemReaderBuilder<String, String, DataStructure<String>> builder = configureJobRepository(
				stringReader(getRedisOptions()).dataStructure());
		return options.configureScanReader(builder).build();
	}

}
