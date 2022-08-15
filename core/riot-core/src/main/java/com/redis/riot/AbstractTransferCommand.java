package com.redis.riot;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.spring.batch.RedisScanSizeEstimator;
import com.redis.spring.batch.RedisScanSizeEstimator.Builder;
import com.redis.spring.batch.reader.PollableItemReader;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import picocli.CommandLine.Mixin;

public abstract class AbstractTransferCommand extends AbstractJobCommand {

	@Mixin
	private TransferOptions transferOptions = new TransferOptions();
	@Mixin
	protected ProgressMonitorOptions progressOptions = new ProgressMonitorOptions();

	protected Builder estimator(JobCommandContext context) {
		return RedisScanSizeEstimator.client(context.getRedisClient());
	}

	protected <I, O> SimpleStepBuilder<I, O> step(JobCommandContext context, String name, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return context.step(name).<I, O>chunk(transferOptions.getChunkSize()).reader(synchronize(reader))
				.processor(processor).writer(writer);
	}

	protected <I, O> Job job(JobCommandContext context, String name, SimpleStepBuilder<I, O> step,
			ProgressMonitor monitor) {
		return job(context, name, step(step, monitor).build()).build();
	}

	protected ProgressMonitor.Builder progressMonitor() {
		return progressOptions.monitor();
	}

	@SuppressWarnings("unchecked")
	protected <I, O> FaultTolerantStepBuilder<I, O> step(SimpleStepBuilder<I, O> step, ProgressMonitor monitor) {
		if (transferOptions.getThreads() > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setCorePoolSize(transferOptions.getThreads());
			taskExecutor.setMaxPoolSize(transferOptions.getThreads());
			taskExecutor.setQueueCapacity(transferOptions.getThreads());
			taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
			taskExecutor.afterPropertiesSet();
			step.taskExecutor(taskExecutor);
			step.throttleLimit(transferOptions.getThreads());
		} else {
			step.taskExecutor(new SyncTaskExecutor());
		}
		if (progressOptions.isEnabled()) {
			step.listener((StepExecutionListener) monitor);
			step.listener((ItemWriteListener<O>) monitor);
		}
		return step.faultTolerant().skipPolicy(skipPolicy(transferOptions.getSkipPolicy()));
	}

	private <I> ItemReader<I> synchronize(ItemReader<I> reader) {
		if (transferOptions.getThreads() > 1) {
			if (reader instanceof PollableItemReader) {
				SynchronizedPollableItemReader<I> pollableReader = new SynchronizedPollableItemReader<>();
				pollableReader.setDelegate((PollableItemReader<I>) reader);
				return pollableReader;
			}
			if (reader instanceof ItemStreamReader) {
				SynchronizedItemStreamReader<I> streamReader = new SynchronizedItemStreamReader<>();
				streamReader.setDelegate((ItemStreamReader<I>) reader);
				return streamReader;
			}
		}
		return reader;
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		return step.faultTolerant();
	}

	private SkipPolicy skipPolicy(TransferOptions.SkipPolicy policy) {
		switch (policy) {
		case ALWAYS:
			return new AlwaysSkipItemSkipPolicy();
		case NEVER:
			return new NeverSkipItemSkipPolicy();
		default:
			return new LimitCheckingItemSkipPolicy(transferOptions.getSkipLimit(),
					Stream.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class,
							TimeoutException.class).collect(Collectors.toMap(t -> t, t -> true)));
		}
	}

}
