package com.redislabs.riot;

import org.springframework.batch.item.redis.support.BoundedItemReader;
import org.springframework.batch.item.redis.support.TransferExecution;
import org.springframework.batch.item.redis.support.TransferExecutionListener;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

public class ProgressReporter implements TransferExecutionListener {

	private final ProgressBar progressBar;

	public ProgressReporter(TransferExecution<?, ?> execution) {
		ProgressBarBuilder builder = new ProgressBarBuilder();
		if (execution.getTransfer().getReader() instanceof BoundedItemReader) {
			builder.setInitialMax(((BoundedItemReader<?>) execution.getTransfer().getReader()).available());
		}
		builder.setTaskName(execution.getTransfer().getName());
		builder.showSpeed();
		this.progressBar = builder.build();
	}

	public void onUpdate(long count) {
		progressBar.setExtraMessage("");
		progressBar.stepTo(count);
	}

	@Override
	public void onMessage(String message) {
		progressBar.setExtraMessage(message);
	}

	@Override
	public void onError(Throwable throwable) {
	}

	@Override
	public void onComplete() {
		progressBar.setExtraMessage("");
		progressBar.close();
	}
}