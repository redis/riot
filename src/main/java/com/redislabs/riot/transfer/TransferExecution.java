package com.redislabs.riot.transfer;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Singular;

@Builder
public class TransferExecution<I, O> {

	@Singular
	private List<FlowExecution<I, O>> flows;

	public Metrics progress() {
		return Metrics.builder().metrics(flows.stream().map(f -> f.progress()).collect(Collectors.toList())).build();
	}

	public boolean isTerminated() {
		boolean finished = true;
		for (FlowExecution<I, O> flowExecution : flows) {
			finished &= flowExecution.isTerminated();
		}
		return finished;
	}

	public void awaitTermination(long timeout, TimeUnit unit) {
		flows.forEach(f -> f.awaitTermination(timeout, unit));
	}

}
