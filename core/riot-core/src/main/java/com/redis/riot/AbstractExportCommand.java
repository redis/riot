package com.redis.riot;

import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisScanSizeEstimator;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand {

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions options = new RedisReaderOptions();

	protected AbstractTaskletStepBuilder<SimpleStepBuilder<DataStructure<String>, O>> step(JobCommandContext context,
			String name, String taskName, ItemProcessor<DataStructure<String>, O> processor, ItemWriter<O> writer) {
		RedisItemReader<String, DataStructure<String>> reader = options
				.configure(RedisItemReader.dataStructure(context.getRedisClient())).build();
		RedisScanSizeEstimator estimator = estimator(context).build();
		return step(context.getJobRunner().step(name), RiotStep.reader(reader).writer(writer).processor(processor)
				.taskName(taskName).max(estimator::execute).build());
	}

}
