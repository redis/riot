package com.redis.riot;

import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.spring.batch.reader.PollableItemReader;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import picocli.CommandLine.Mixin;

public abstract class AbstractTransferCommand extends AbstractJobCommand {

	@Mixin
	private TransferOptions options = new TransferOptions();
	@Mixin
	protected ProgressMonitorOptions progressOptions = new ProgressMonitorOptions();

	public void setTransferOptions(TransferOptions transferOptions) {
		this.options = transferOptions;
	}

	public void setProgressOptions(ProgressMonitorOptions progressOptions) {
		this.progressOptions = progressOptions;
	}

	protected <I, O> SimpleStepBuilder<I, O> step(JobCommandContext context, String name, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return context.step(name).<I, O>chunk(options.getChunkSize()).reader(throttle(synchronize(reader)))
				.processor(processor).writer(writer);
	}

	private <I> ItemReader<I> throttle(ItemReader<I> reader) {
		if (reader instanceof PollableItemReader) {
			return reader;
		}
		return ThrottledItemReader.create(reader, Duration.ofMillis(options.getSleep()));
	}

	protected <I, O> Job job(JobCommandContext context, String name, SimpleStepBuilder<I, O> step,
			ProgressMonitor monitor) {
		return job(context, name, step(step, monitor).build()).build();
	}

	protected ProgressMonitor.Builder progressMonitor() {
		return progressOptions.monitor();
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> step(SimpleStepBuilder<I, O> step, ProgressMonitor monitor) {
		if (options.getThreads() > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setCorePoolSize(options.getThreads());
			taskExecutor.setMaxPoolSize(options.getThreads());
			taskExecutor.setQueueCapacity(options.getThreads());
			taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
			taskExecutor.afterPropertiesSet();
			step.taskExecutor(taskExecutor);
			step.throttleLimit(options.getThreads());
		} else {
			step.taskExecutor(new SyncTaskExecutor());
		}
		if (progressOptions.isEnabled()) {
			step.listener((StepExecutionListener) monitor);
			step.listener((ItemWriteListener<Object>) monitor);
		}
		SkipPolicy skipPolicy = options.getSkipPolicy().getSkipPolicy();
		if (skipPolicy instanceof LimitCheckingItemSkipPolicy) {
			LimitCheckingItemSkipPolicy limitSkipPolicy = (LimitCheckingItemSkipPolicy) skipPolicy;
			limitSkipPolicy.setSkippableExceptionMap(
					Stream.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class,
							TimeoutException.class).collect(Collectors.toMap(t -> t, t -> true)));
		}
		return step.faultTolerant().skipPolicy(options.getSkipPolicy().getSkipPolicy());
	}

	private <I> ItemReader<I> synchronize(ItemReader<I> reader) {
		if (options.getThreads() > 1) {
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

}
