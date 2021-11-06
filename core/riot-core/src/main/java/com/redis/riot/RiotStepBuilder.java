package com.redis.riot;

import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

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
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.FlushingStepBuilder;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
@Accessors(fluent = true)
public class RiotStepBuilder<I, O> {

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
