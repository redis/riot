package com.redis.riot.cli.common;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.riot.cli.Riot;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.ParentCommand;

public abstract class AbstractExportCommand extends AbstractTransferCommand {

	@ParentCommand
	private Riot parent;

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions readerOptions = new RedisReaderOptions();

	@Override
	protected RedisOptions getRedisOptions() {
		return parent.getRedisOptions();
	}

	public void setReaderOptions(RedisReaderOptions readerOptions) {
		this.readerOptions = readerOptions;
	}

	protected RedisItemReader<String, DataStructure<String>> reader(JobCommandContext context) {
		return context.reader().readerOptions(readerOptions.readerOptions()).scanOptions(readerOptions.scanOptions())
				.dataStructure();
	}

	protected <I, O> Job job(JobCommandContext context, String name, SimpleStepBuilder<I, O> step, String task) {
		ScanSizeEstimator estimator = ScanSizeEstimator.client(context.getRedisClient())
				.options(readerOptions.scanSizeEstimatorOptions()).build();
		ProgressMonitor monitor = progressMonitor().task(task).initialMax(estimator::execute).build();
		return context.job(name).start(step(step, monitor).build()).build();
	}

}
