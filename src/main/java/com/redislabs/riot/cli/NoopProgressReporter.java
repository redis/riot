package com.redislabs.riot.cli;

import com.redislabs.riot.transfer.Metrics;

public class NoopProgressReporter implements ProgressReporter {

	@Override
	public void start() {
	}

	@Override
	public void onUpdate(Metrics update) {
	}

	@Override
	public void stop() {
	}

}
