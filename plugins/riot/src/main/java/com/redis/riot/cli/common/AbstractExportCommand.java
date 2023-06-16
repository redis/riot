package com.redis.riot.cli.common;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractCommand {

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions readerOptions = new RedisReaderOptions();

	public void setReaderOptions(RedisReaderOptions readerOptions) {
		this.readerOptions = readerOptions;
	}

	private RedisItemReader<String, String, DataStructure<String>> reader(CommandContext context) {
		RedisItemReader<String, String, DataStructure<String>> reader = reader(context.getRedisClient(), readerOptions)
				.dataStructure();
		reader.setName(commandName() + "-reader");
		return reader;
	}

	protected <O> SimpleStepBuilder<DataStructure<String>, O> step(CommandContext context, String task,
			ItemWriter<O> writer) {
		SimpleStepBuilder<DataStructure<String>, O> step = step();
		ScanSizeEstimator estimator = estimator(context.getRedisClient(), readerOptions);
		StepProgressMonitor monitor = progressMonitor(task).withInitialMax(estimator);
		monitor.register(step);
		step.reader(reader(context));
		step.writer(writer);
		return step;
	}

}
