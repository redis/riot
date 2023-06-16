package com.redis.riot.cli.common;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

public class StepProgressMonitor implements StepExecutionListener, ItemWriteListener<Object> {

	public static final long DEFAULT_INITIAL_MAX = -1;
	public static final ProgressBarStyle DEFAULT_STYLE = ProgressBarStyle.COLORFUL_UNICODE_BLOCK;

	private ProgressBarStyle style = DEFAULT_STYLE;
	private String task;
	private Duration updateInterval = Duration.ofMillis(300);
	private LongSupplier initialMax = () -> DEFAULT_INITIAL_MAX;
	private Optional<Supplier<String>> extraMessage = Optional.empty();
	private ProgressBar progressBar;

	public StepProgressMonitor withStyle(ProgressBarStyle style) {
		this.style = style;
		return this;
	}

	public StepProgressMonitor withTask(String task) {
		this.task = task;
		return this;
	}

	public StepProgressMonitor withUpdateInterval(Duration interval) {
		this.updateInterval = interval;
		return this;
	}

	public StepProgressMonitor withInitialMax(long max) {
		return withInitialMax(() -> max);
	}

	public StepProgressMonitor withInitialMax(LongSupplier max) {
		this.initialMax = max;
		return this;
	}

	public StepProgressMonitor withExtraMessage(Supplier<String> message) {
		this.extraMessage = Optional.of(message);
		return this;
	}

	public StepProgressMonitor withExtraMessage(String message) {
		return withExtraMessage(() -> message);
	}

	public void register(SimpleStepBuilder<?, ?> step) {
		if (updateInterval.isNegative() || updateInterval.isZero()) {
			return;
		}
		step.listener((StepExecutionListener) this);
		step.listener((ItemWriteListener<Object>) this);
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		ProgressBarBuilder builder = new ProgressBarBuilder();
		builder.setStyle(style);
		builder.setInitialMax(initialMax.getAsLong());
		builder.setUpdateIntervalMillis(Math.toIntExact(updateInterval.toMillis()));
		builder.setTaskName(task);
		builder.showSpeed();
		progressBar = builder.build();
	}

	@Override
	public synchronized ExitStatus afterStep(StepExecution stepExecution) {
		if (stepExecution.getStatus() != BatchStatus.FAILED) {
			progressBar.close();
		}
		return null;
	}

	@Override
	public void beforeWrite(List<? extends Object> items) {
		// do nothing
	}

	@Override
	public void afterWrite(List<? extends Object> items) {
		progressBar.stepBy(items.size());
		extraMessage.map(Supplier::get).ifPresent(progressBar::setExtraMessage);
	}

	@Override
	public void onWriteError(Exception exception, List<? extends Object> items) {
		// do nothing
	}

}