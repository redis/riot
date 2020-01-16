package com.redislabs.riot.cli;

import com.redislabs.riot.transfer.Metrics;

public interface ProgressReporter {

	void start();

	void onUpdate(Metrics update);

	void stop();

}
