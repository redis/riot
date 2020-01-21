package com.redislabs.riot.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TransferExecution {

	private List<FlowExecution> flowExecutions = new ArrayList<>();

	public TransferExecution flowExecution(FlowExecution flowExecution) {
		this.flowExecutions.add(flowExecution);
		return this;
	}

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
