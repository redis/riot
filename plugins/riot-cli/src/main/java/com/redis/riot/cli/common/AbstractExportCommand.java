package com.redis.riot.cli.common;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractCommand {

	@ArgGroup(exclusive = false, heading = "Reader options%n")
	private RedisReaderOptions readerOptions = new RedisReaderOptions();

	public void setReaderOptions(RedisReaderOptions readerOptions) {
		this.readerOptions = readerOptions;
	}

	protected RedisItemReader<String, String, DataStructure<String>> reader(CommandContext context) {
		return context.dataStructureReader(StringCodec.UTF8).options(readerOptions.readerOptions())
				.scanOptions(readerOptions.scanOptions()).build();
	}

	protected <I, O> Job job(CommandContext context, SimpleStepBuilder<I, O> step, String task) {
		ScanSizeEstimator estimator = new ScanSizeEstimator(context.getRedisClient(),
				readerOptions.scanSizeEstimatorOptions());
		ProgressMonitor monitor = progressMonitor().task(task).initialMax(estimator).build();
		return context.getJobRunner().job(commandName()).start(step(step, monitor).build()).build();
	}

}
