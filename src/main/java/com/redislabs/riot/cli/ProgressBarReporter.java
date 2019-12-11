package com.redislabs.riot.cli;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

public class ProgressBarReporter implements ProgressReporter {

	private ProgressBar pb;

	public ProgressBarReporter(Integer initialMax, String taskName) {
		this(initialMax, taskName, null);
	}

	public ProgressBarReporter(Integer initialMax, String taskName, String unitName) {
		ProgressBarBuilder pbb = new ProgressBarBuilder().setInitialMax(initialMax == null ? -1 : initialMax)
				.setTaskName(taskName).showSpeed();
		if (unitName != null) {
			pbb.setUnit(" " + unitName + "s", 1);
		}
		this.pb = pbb.build();
	}

	@Override
	public void onUpdate(long writeCount, int runningThreads) {
		pb.stepTo(writeCount);
		if (runningThreads > 1) {
			pb.setExtraMessage(" (" + runningThreads + " threads)");
		}
	}
}
