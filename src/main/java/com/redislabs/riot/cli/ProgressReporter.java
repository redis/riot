package com.redislabs.riot.cli;

public interface ProgressReporter {

	void onUpdate(long writeCount, int runningThreads);

}
