package com.redis.riot;

import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.riot.TransferOptions.Progress;
import com.redis.spring.batch.RedisScanSizeEstimator;
import com.redis.spring.batch.reader.PollableItemReader;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import picocli.CommandLine.Mixin;

public abstract class AbstractTransferCommand extends AbstractJobCommand {

	private static final Logger log = Logger.getLogger(AbstractTransferCommand.class.getName());

	@Mixin
	protected TransferOptions transferOptions = new TransferOptions();

	protected RedisScanSizeEstimator.Builder estimator(JobCommandContext context) {
		return RedisScanSizeEstimator.client(context.getRedisClient());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <I, O> FaultTolerantStepBuilder<I, O> step(StepBuilder stepBuilder, RiotStep<I, O> riotStep) {
		SimpleStepBuilder<I, O> step = stepBuilder.chunk(transferOptions.getChunkSize());
		step.reader(riotStep.getReader()).writer(riotStep.getWriter()).processor(riotStep.getProcessor());
		if (transferOptions.getProgress() != Progress.NONE) {
			ProgressMonitor.ProgressMonitorBuilder monitorBuilder = ProgressMonitor.builder();
			monitorBuilder.style(transferOptions.getProgress());
			monitorBuilder.taskName(riotStep.getTaskName());
			monitorBuilder.initialMax(riotStep.getMax());
			monitorBuilder.updateInterval(Duration.ofMillis(transferOptions.getProgressUpdateIntervalMillis()));
			monitorBuilder.extraMessage(riotStep.getMessage());
			ProgressMonitor monitor = monitorBuilder.build();
			step.listener((StepExecutionListener) monitor);
			step.listener((ItemWriteListener) monitor);
		}
		FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant()
				.skipPolicy(skipPolicy(transferOptions.getSkipPolicy()));
		if (transferOptions.getThreads() > 1) {
			ftStep.reader(synchronize(riotStep.getReader()));
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setCorePoolSize(transferOptions.getThreads());
			taskExecutor.setMaxPoolSize(transferOptions.getThreads());
			taskExecutor.setQueueCapacity(transferOptions.getThreads());
			taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
			taskExecutor.afterPropertiesSet();
			log.log(Level.FINE, "Created pooled task executor of size {0}", taskExecutor.getCorePoolSize());
			ftStep.taskExecutor(taskExecutor).throttleLimit(transferOptions.getThreads());
		} else {
			ftStep.taskExecutor(new SyncTaskExecutor());
		}
		return ftStep;
	}

	private <I> ItemReader<I> synchronize(ItemReader<I> reader) {
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
