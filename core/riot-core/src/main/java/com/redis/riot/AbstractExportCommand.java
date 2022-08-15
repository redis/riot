package com.redis.riot;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractTransferCommand {

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions redisReaderOptions = new RedisReaderOptions();

	protected RedisItemReader<String, DataStructure<String>> reader(JobCommandContext context) {
		return redisReaderOptions.configure(RedisItemReader.dataStructure(context.getRedisClient())).build();
	}

	protected <I, O> Job job(JobCommandContext context, String name, SimpleStepBuilder<I, O> step, String task) {
		ProgressMonitor monitor = progressMonitor().task(task).initialMax(estimator(context).build()::execute).build();
		return super.job(context, name, step, monitor);
	}

}
