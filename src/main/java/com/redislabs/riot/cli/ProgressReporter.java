package com.redislabs.riot.cli;

import com.redislabs.riot.TransferExecution.ProgressUpdate;

public interface ProgressReporter {

	void start();

	void onUpdate(ProgressUpdate update);

	void stop();

}
