package com.redis.riot.cli;

import java.time.Duration;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.ThrottledItemReader;

import picocli.CommandLine.Mixin;

public abstract class AbstractTransferCommand extends AbstractJobCommand {

	@Mixin
	private TransferOptions options = new TransferOptions();

	public TransferOptions getTransferOptions() {
		return options;
	}

	public void setTransferOptions(TransferOptions options) {
		this.options = options;
	}

	protected <I, O> SimpleStepBuilder<I, O> step(JobCommandContext context, String name, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		SimpleStepBuilder<I, O> step = context.step(name).<I, O>chunk(options.getChunkSize())
				.reader(throttle(synchronize(reader))).processor(processor).writer(writer);
		JobRunner.multiThreaded(step, options.getThreads());
		return step;
	}

	private <I> ItemReader<I> synchronize(ItemReader<I> reader) {
		if (options.getThreads() > 1) {
			return JobRunner.synchronize(reader);
		}
		return reader;
	}

	private <I> ItemReader<I> throttle(ItemReader<I> reader) {
		if (reader instanceof PollableItemReader) {
			return reader;
		}
		return new ThrottledItemReader<>(reader, Duration.ofMillis(options.getSleep()));
	}

	protected ProgressMonitor.Builder progressMonitor() {
		return options.progressMonitor();
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> step(SimpleStepBuilder<I, O> step, ProgressMonitor monitor) {
		if (options.isProgressEnabled()) {
			step.listener((StepExecutionListener) monitor);
			step.listener((ItemWriteListener<Object>) monitor);
		}
		return JobRunner.faultTolerant(step, options.faultToleranceOptions());
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		return step.faultTolerant();
	}

}
