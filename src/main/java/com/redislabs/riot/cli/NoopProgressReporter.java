package com.redislabs.riot.cli;

import com.redislabs.riot.TransferExecution.ProgressUpdate;

public class NoopProgressReporter implements ProgressReporter {

	@Override
	public void start() {
	}

	@Override
	public void onUpdate(ProgressUpdate update) {
	}

	@Override
	public void stop() {
	}

}
