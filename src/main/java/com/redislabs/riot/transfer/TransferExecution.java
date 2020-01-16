package com.redislabs.riot.transfer;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Singular;

@Builder
public class TransferExecution {

	@Singular
	private List<FlowExecution> flowExecutions;

	public Metrics progress() {
		return Metrics.create(flowExecutions.stream().map(f -> f.progress()).collect(Collectors.toList()));
	}

	public boolean isTerminated() {
		boolean finished = true;
		for (FlowExecution flowExecution : flowExecutions) {
			finished &= flowExecution.isTerminated();
		}
		return finished;
	}

	public void awaitTermination(long timeout, TimeUnit unit) {
		for (FlowExecution execution : flowExecutions) {
			execution.awaitTermination(timeout, unit);
		}
	}

}
