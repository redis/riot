package com.redis.riot;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractTransferCommand {

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions readerArgs = new RedisReaderOptions();

	protected RedisItemReader<String, DataStructure<String>> reader(JobCommandContext context) {
		return RedisItemReader.dataStructure(context.pool(), context.getJobRunner()).options(readerArgs.readerOptions())
				.build();
	}

	protected <I, O> Job job(JobCommandContext context, String name, SimpleStepBuilder<I, O> step, String task) {
		ScanSizeEstimator estimator = ScanSizeEstimator.builder(context.pool()).options(readerArgs.estimatorOptions())
				.build();
		return super.job(context, name, step, progressMonitor().task(task).initialMax(estimator::execute).build());
	}

}
