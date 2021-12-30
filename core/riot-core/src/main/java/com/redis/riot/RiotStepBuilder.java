package com.redis.riot;

import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.FlushingStepBuilder;
import com.redis.spring.batch.support.PollableItemReader;

public class RiotStepBuilder<I, O> {

	private static final Logger log = LoggerFactory.getLogger(RiotStepBuilder.class);

	private final StepBuilder stepBuilder;
	private final TransferOptions options;

	private String taskName;
	private ItemReader<I> reader;
	private ItemProcessor<I, O> processor;
	private ItemWriter<O> writer;
	private Supplier<String> extraMessage;
	private Supplier<Long> initialMax;
	private FlushingTransferOptions flushingOptions;

	public RiotStepBuilder(StepBuilder stepBuilder, TransferOptions options) {
		this.stepBuilder = stepBuilder;
		this.options = options;
	}

	public RiotStepBuilder<I, O> taskName(String taskName) {
		this.taskName = taskName;
		return this;
	}

	public RiotStepBuilder<I, O> reader(ItemReader<I> reader) {
		this.reader = reader;
		return this;
	}

	public RiotStepBuilder<I, O> processor(ItemProcessor<I, O> processor) {
		this.processor = processor;
		return this;
	}

	public RiotStepBuilder<I, O> writer(ItemWriter<O> writer) {
		this.writer = writer;
		return this;
	}

	public RiotStepBuilder<I, O> extraMessage(Supplier<String> extraMessage) {
		this.extraMessage = extraMessage;
		return this;
	}

	public RiotStepBuilder<I, O> initialMax(Supplier<Long> initialMax) {
		this.initialMax = initialMax;
		return this;
	}

	public RiotStepBuilder<I, O> flushingOptions(FlushingTransferOptions flushingOptions) {
		this.flushingOptions = flushingOptions;
		return this;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public FaultTolerantStepBuilder<I, O> build() {
		SimpleStepBuilder<I, O> step = stepBuilder.<I, O>chunk(options.getChunkSize()).reader(reader)
				.processor(processor).writer(writer);
		if (options.getProgress() != TransferOptions.Progress.NONE) {
			ProgressMonitor.ProgressMonitorBuilder monitorBuilder = ProgressMonitor.builder();
			monitorBuilder.style(options.getProgress());
			monitorBuilder.taskName(taskName);
			monitorBuilder.initialMax(initialMax);
			monitorBuilder.updateInterval(Duration.ofMillis(options.getProgressUpdateIntervalMillis()));
			monitorBuilder.extraMessage(extraMessage);
			ProgressMonitor monitor = monitorBuilder.build();
			step.listener((StepExecutionListener) monitor);
			step.listener((ItemWriteListener) monitor);
		}
		FaultTolerantStepBuilder<I, O> ftStep = faultTolerant(step).skipPolicy(skipPolicy(options.getSkipPolicy()));
		if (options.getThreads() > 1) {
			ftStep.reader(synchronize(reader));
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setCorePoolSize(options.getThreads());
			taskExecutor.setMaxPoolSize(options.getThreads());
			taskExecutor.setQueueCapacity(options.getThreads());
			taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
			taskExecutor.afterPropertiesSet();
			log.debug("Created pooled task executor of size {}", taskExecutor.getCorePoolSize());
			ftStep.taskExecutor(taskExecutor).throttleLimit(options.getThreads());
		} else {
			ftStep.taskExecutor(new SyncTaskExecutor());
		}
		return ftStep;
	}

	private ItemReader<I> synchronize(ItemReader<I> reader) {
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

	private FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		if (flushingOptions == null) {
			return step.faultTolerant();
		}
		FlushingStepBuilder<I, O> builder = new FlushingStepBuilder<>(step)
				.flushingInterval(flushingOptions.getFlushIntervalDuration());
		if (flushingOptions.getIdleTimeoutDuration() != null) {
			builder.idleTimeout(flushingOptions.getIdleTimeoutDuration());
		}
		return builder;
	}

	private SkipPolicy skipPolicy(TransferOptions.SkipPolicy policy) {
		switch (policy) {
		case ALWAYS:
			return new AlwaysSkipItemSkipPolicy();
		case NEVER:
			return new NeverSkipItemSkipPolicy();
		default:
			return RedisItemReader.limitCheckingSkipPolicy(options.getSkipLimit());
		}
	}

}
