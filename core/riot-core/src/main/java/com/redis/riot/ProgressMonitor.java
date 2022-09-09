package com.redis.riot;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.util.Assert;

import com.redis.spring.batch.common.Utils;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

public class ProgressMonitor implements StepExecutionListener, ItemWriteListener<Object> {

	public enum Style {
		ASCII, COLOR, BW, NONE
	}

	private final Style style;
	private final String task;
	private final Duration updateInterval;
	private final Optional<Supplier<Long>> initialMax;
	protected ProgressBar progressBar;

	private ProgressMonitor(Builder builder) {
		this.style = builder.style;
		this.task = builder.task;
		this.updateInterval = builder.updateInterval;
		this.initialMax = builder.initialMax;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		ProgressBarBuilder builder = new ProgressBarBuilder();
		builder.setStyle(progressBarStyle());
		initialMax.ifPresent(m -> {
			Long initialMaxValue = m.get();
			if (initialMaxValue != null) {
				builder.setInitialMax(initialMaxValue);
			}
		});
		builder.setUpdateIntervalMillis(Math.toIntExact(updateInterval.toMillis()));
		builder.setTaskName(task);
		builder.showSpeed();
		progressBar = builder.build();
	}

	private ProgressBarStyle progressBarStyle() {
		switch (style) {
		case ASCII:
			return ProgressBarStyle.ASCII;
		case BW:
			return ProgressBarStyle.UNICODE_BLOCK;
		default:
			return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
		}
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
	}

	@Override
	public void onWriteError(Exception exception, List<? extends Object> items) {
		// do nothing
	}

	private static class ExtraMessageProgressMonitor extends ProgressMonitor {

		private final Supplier<String> extraMessage;

		public ExtraMessageProgressMonitor(Builder builder) {
			super(builder);
			this.extraMessage = builder.extraMessage.get();
		}

		@Override
		public void afterWrite(List<? extends Object> items) {
			super.afterWrite(items);
			progressBar.setExtraMessage(extraMessage.get());
		}
	}

	public static Builder style(Style style) {
		return new Builder(style);
	}

	public static class Builder {

		private final Style style;
		private String task;
		private Duration updateInterval = Duration.ofMillis(300);
		private Optional<Supplier<Long>> initialMax = Optional.empty();
		private Optional<Supplier<String>> extraMessage = Optional.empty();

		public Builder(Style style) {
			this.style = style;
		}

		public ProgressMonitor.Builder task(String task) {
			this.task = task;
			return this;
		}

		public ProgressMonitor.Builder updateInterval(Duration updateInterval) {
			Utils.assertPositive(updateInterval, "Update interval");
			this.updateInterval = updateInterval;
			return this;
		}

		public ProgressMonitor.Builder initialMax(Supplier<Long> initialMax) {
			Assert.notNull(initialMax, "InitialMax supplier must not be null");
			this.initialMax = Optional.of(initialMax);
			return this;
		}

		public ProgressMonitor.Builder extraMessage(Supplier<String> extraMessage) {
			this.extraMessage = Optional.of(extraMessage);
			return this;
		}

		public ProgressMonitor build() {
			if (extraMessage.isPresent()) {
				return new ExtraMessageProgressMonitor(this);
			}
			return new ProgressMonitor(this);
		}

	}

}