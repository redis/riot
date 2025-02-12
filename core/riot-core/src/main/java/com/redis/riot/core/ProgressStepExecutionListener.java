package com.redis.riot.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.Chunk;

import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

/**
 * Listener tracking writer or step progress with by a progress bar.
 * 
 * @author Julien Ruaux
 * @since 3.1.2
 */
@SuppressWarnings("rawtypes")
public class ProgressStepExecutionListener<I, O> implements StepExecutionListener, ItemWriteListener {

	private final Step<I, O> step;

	private ProgressArgs progressArgs = new ProgressArgs();

	private ProgressBar progressBar;

	public ProgressStepExecutionListener(Step<I, O> step) {
		this.step = step;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
		progressBarBuilder.setTaskName(step.getTaskName());
		progressBarBuilder.setStyle(progressBarStyle());
		progressBarBuilder
				.setUpdateIntervalMillis(Math.toIntExact(progressArgs.getUpdateInterval().getValue().toMillis()));
		progressBarBuilder.showSpeed();
		if (progressArgs.getStyle() == ProgressStyle.LOG) {
			Logger logger = LoggerFactory.getLogger(getClass());
			progressBarBuilder.setConsumer(new DelegatingProgressBarConsumer(logger::info));
		}
		progressBarBuilder.setInitialMax(step.maxItemCount());
		this.progressBar = progressBarBuilder.build();
	}

	private ProgressBarStyle progressBarStyle() {
		switch (progressArgs.getStyle()) {
		case BAR:
			return ProgressBarStyle.COLORFUL_UNICODE_BAR;
		case BLOCK:
			return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
		default:
			return ProgressBarStyle.ASCII;
		}
	}

	@Override
	public void afterWrite(Chunk items) {
		if (progressBar != null) {
			progressBar.stepBy(items.size());
			progressBar.setExtraMessage(step.statusMessage());
		}
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (progressBar != null) {
			if (!stepExecution.getStatus().isUnsuccessful()) {
				progressBar.stepTo(progressBar.getMax());
			}
			progressBar.close();
			progressBar = null;
		}
		return stepExecution.getExitStatus();
	}

	public ProgressArgs getProgressArgs() {
		return progressArgs;
	}

	public void setProgressArgs(ProgressArgs args) {
		this.progressArgs = args;
	}

}
