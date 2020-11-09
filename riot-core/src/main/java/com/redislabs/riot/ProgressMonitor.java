package com.redislabs.riot;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.redis.support.ProgressReporter;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

public class ProgressMonitor implements Runnable {

    private final List<? extends Transfer<?, ?>> transfers;
    private boolean stopped;

    public ProgressMonitor(List<? extends Transfer<?, ?>> transfers) {
	this.transfers = transfers;
    }

    @Override
    public void run() {
	Map<ProgressReporter, ProgressBar> progressBars = new LinkedHashMap<>();
	try {
	    for (Transfer<?, ?> transfer : transfers) {
		ProgressBarBuilder builder = new ProgressBarBuilder();
		Long total = transfer.getTotal();
		if (total != null) {
		    builder.setInitialMax(total);
		}
		builder.setTaskName(transfer.getName());
		builder.showSpeed();
		progressBars.put(transfer, builder.build());
	    }
	    do {
		progressBars.forEach(this::update);
		Thread.sleep(100);
	    } while (!stopped);
	    progressBars.forEach(this::update);
	} catch (InterruptedException e) {
	    // ignore
	} finally {
	    progressBars.values().forEach(ProgressBar::close);
	}
    }

    private void update(ProgressReporter reporter, ProgressBar progressBar) {
	progressBar.stepTo(reporter.getDone());
    }

    public void stop() {
	this.stopped = true;
    }

}
