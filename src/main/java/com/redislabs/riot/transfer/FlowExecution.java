package com.redislabs.riot.transfer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Builder;

public class FlowExecution {

	private List<FlowThread> threads;
	private ExecutorService executor;

	@Builder
	private FlowExecution(List<FlowThread> threads, ExecutorService executor) {
		this.threads = threads;
		this.executor = executor;
	}

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