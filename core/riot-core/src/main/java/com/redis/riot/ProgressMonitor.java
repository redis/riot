package com.redis.riot;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.util.Assert;

import com.redis.riot.TransferOptions.Progress;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

@SuppressWarnings("rawtypes")
public class ProgressMonitor implements StepExecutionListener, ItemWriteListener {

	private static final Logger log = LoggerFactory.getLogger(ProgressMonitor.class);

	private final TransferOptions.Progress style;
	private final String taskName;
	private final Duration updateInterval;
	private final Supplier<Long> initialMax;
	protected ProgressBar progressBar;

	public ProgressMonitor(TransferOptions.Progress style, String taskName, Duration updateInterval,
			Supplier<Long> initialMax) {
		Assert.notNull(style, "A progress style is required");
		Assert.notNull(taskName, "A task name is required");
		Assert.notNull(updateInterval, "Update interval is required");
		Assert.isTrue(!updateInterval.isZero(), "Update interval must not be zero");
		Assert.isTrue(!updateInterval.isNegative(), "Update interval must be greater than zero");
		this.style = style;
		this.taskName = taskName;
		this.updateInterval = updateInterval;
		this.initialMax = initialMax;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		ProgressBarBuilder builder = new ProgressBarBuilder();
		builder.setStyle(progressBarStyle());
		if (initialMax != null) {
			Long initialMaxValue = initialMax.get();
			if (initialMaxValue != null) {
				log.debug("Setting initial max to {}", initialMaxValue);
				builder.setInitialMax(initialMaxValue);
			}
		}
		log.debug("Setting update interval to {}", updateInterval);
		builder.setUpdateIntervalMillis(Math.toIntExact(updateInterval.toMillis()));
		log.debug("Setting task name to {}", taskName);
		builder.setTaskName(taskName);
		builder.showSpeed();
		log.debug("Opening progress bar");
		this.progressBar = builder.build();
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
	public ExitStatus afterStep(StepExecution stepExecution) {
		log.debug("Closing progress bar");
		progressBar.close();
		return null;
	}

	@Override
	public void beforeWrite(List items) {
		// do nothing
	}

	@Override
	public void afterWrite(List items) {
		progressBar.stepBy(items.size());
	}

	@Override
	public void onWriteError(Exception exception, List items) {
		// do nothing
	}

	private static class ExtraMessageProgressMonitor extends ProgressMonitor {

		private final Supplier<String> extraMessage;

		public ExtraMessageProgressMonitor(TransferOptions.Progress style, String taskName, Duration updateInterval,
				Supplier<Long> initialMax, Supplier<String> extraMessage) {
			super(style, taskName, updateInterval, initialMax);
			this.extraMessage = extraMessage;
		}

		@Override
		public void afterWrite(List items) {
			super.afterWrite(items);
			progressBar.setExtraMessage(extraMessage.get());
		}
	}

	public static ProgressMonitorBuilder builder() {
		return new ProgressMonitorBuilder();
	}

	public static class ProgressMonitorBuilder {

		private Progress style;
		private String taskName;
		private Duration updateInterval;
		private Supplier<Long> initialMax;
		private Supplier<String> extraMessage;

		public ProgressMonitor.ProgressMonitorBuilder style(Progress style) {
			this.style = style;
			return this;
		}

		public ProgressMonitor.ProgressMonitorBuilder taskName(String taskName) {
			this.taskName = taskName;
			return this;
		}

		public ProgressMonitor.ProgressMonitorBuilder updateInterval(Duration updateInterval) {
			this.updateInterval = updateInterval;
			return this;
		}

		public ProgressMonitor.ProgressMonitorBuilder initialMax(Supplier<Long> initialMax) {
			this.initialMax = initialMax;
			return this;
		}

		public ProgressMonitor.ProgressMonitorBuilder extraMessage(Supplier<String> extraMessage) {
			this.extraMessage = extraMessage;
			return this;
		}

		public ProgressMonitor build() {
			if (extraMessage == null) {
				return new ProgressMonitor(style, taskName, updateInterval, initialMax);
			}
			return new ExtraMessageProgressMonitor(style, taskName, updateInterval, initialMax, extraMessage);
		}

	}

}