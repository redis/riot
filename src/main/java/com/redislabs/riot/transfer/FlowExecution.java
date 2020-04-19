package com.redislabs.riot.transfer;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Builder
@Slf4j
public class FlowExecution<I, O> {

	@Getter
	@Singular
	private List<FlowThread<I, O>> threads;
	private ExecutorService executor;
	private List<Future<?>> futures;

	public void stop() {
		threads.forEach(t -> t.stop());
	}

	public Metrics progress() {
		return Metrics.builder().metrics(threads.stream().map(t -> t.progress()).collect(Collectors.toList())).build();
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

    public FlowExecution<I,O> execute() {
		this.executor = Executors.newFixedThreadPool(threads.size());
		this.futures = new ArrayList<>(threads.size());
		for (FlowThread<I, O> thread : threads) {
			this.futures.add(executor.submit(thread));
		}
		executor.shutdown();
		return this;
	}

}