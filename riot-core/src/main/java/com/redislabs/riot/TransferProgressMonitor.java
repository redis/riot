package com.redislabs.riot;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

public class TransferProgressMonitor implements Runnable {

    private final Transfer<?, ?> transfer;
    private final String taskName;
    private boolean stopped;

    public TransferProgressMonitor(Transfer<?, ?> transfer, String taskName) {
	this.transfer = transfer;
	this.taskName = taskName;
    }

    @Override
    public void run() {
	ProgressBarBuilder builder = new ProgressBarBuilder();
	Long total = transfer.getTotal();
	if (total != null) {
	    builder.setInitialMax(total);
	}
	builder.setTaskName(taskName);
	builder.showSpeed();
	try (ProgressBar progressBar = builder.build()) {
	    while (!stopped) {
		try {
		    Thread.sleep(100);
		} catch (InterruptedException e) {
		    return;
		}
		progressBar.stepTo(transfer.getDone());
	    }
	    progressBar.stepTo(transfer.getDone());
	}
    }

    public void stop() {
	this.stopped = true;
    }

}
