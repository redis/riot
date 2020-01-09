package com.redislabs.riot.cli;

import com.redislabs.riot.TransferExecution.ProgressUpdate;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

public class ProgressBarReporter implements ProgressReporter {

	private ProgressBarBuilder builder = new ProgressBarBuilder();
	private ProgressBar bar;

	public ProgressBarReporter initialMax(long initialMax) {
		builder.setInitialMax(initialMax);
		return this;
	}

	public ProgressBarReporter taskName(String taskName) {
		builder.setTaskName(taskName);
		return this;
	}

	public ProgressBarReporter unitName(String unitName) {
		builder.setUnit(" " + unitName + "s", 1);
		return this;
	}

	@Override
	public void start() {
		this.bar = builder.build();
	}

	@Override
	public void onUpdate(ProgressUpdate update) {
		bar.stepTo(update.getWrites());
		if (update.getRunningThreads() > 1) {
			bar.setExtraMessage(" (" + update.getRunningThreads() + " threads)");
		}
	}

	@Override
	public void stop() {
		this.bar.close();
	}

}
