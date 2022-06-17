package com.redis.riot;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand {

	private static final Logger log = LoggerFactory.getLogger(AbstractExportCommand.class);

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions options = new RedisReaderOptions();

	protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(String name, String taskName,
			Optional<ItemProcessor<DataStructure<String>, O>> processor, ItemWriter<O> writer) throws Exception {
		return step(RiotStep.reader(reader()).writer(writer).processor(processor).name(name).taskName(taskName)
				.max(this::initialMax).build());
	}

	private Long initialMax() {
		try {
			return estimator().build().call();
		} catch (Exception e) {
			log.warn("Could not estimate scan size", e);
			return null;
		}
	}

	private final RedisItemReader<String, DataStructure<String>> reader() throws Exception {
		return options.configureScanReader(stringReader(getRedisOptions()).dataStructure()).build();
	}

}
