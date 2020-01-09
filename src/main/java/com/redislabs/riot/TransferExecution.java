package com.redislabs.riot;

import java.util.List;
import java.util.concurrent.ExecutorService;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TransferExecution<I, O> {

	private final List<TransferThread<I, O>> threads;
	private final ExecutorService executor;

	public ProgressUpdate progress() {
		long writes = 0;
		int runningThreads = 0;
		for (TransferThread<?, ?> thread : threads) {
			writes += thread.getWriteCount();
			if (thread.isRunning()) {
				runningThreads++;
			}
		}
		ProgressUpdate update = new ProgressUpdate();
		update.setWrites(writes);
		update.setRunningThreads(runningThreads);
		return update;
	}

	public boolean isFinished() {
		return executor.isTerminated();
	}

	public static @Data class ProgressUpdate {

		private long writes;
		private int runningThreads;
	}
}
