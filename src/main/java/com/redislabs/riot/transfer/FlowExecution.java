package com.redislabs.riot.transfer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Setter;

public class FlowExecution {

	private @Setter List<FlowThread> threads;
	private @Setter ExecutorService executor;

	public void stop() {
		threads.forEach(t -> t.stop());
	}

	public Metrics progress() {
		return Metrics.create(threads.stream().map(t -> t.progress()).collect(Collectors.toList()));
	}

	public boolean isTerminated() {
		return executor.isTerminated();
	}

	public void awaitTermination(long timeout, TimeUnit unit) {
		try {
			if (!executor.awaitTermination(timeout, unit)) {
				executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			executor.shutdownNow();
		}
	}

}