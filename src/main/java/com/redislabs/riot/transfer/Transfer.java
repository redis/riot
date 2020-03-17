package com.redislabs.riot.transfer;

import java.util.ArrayList;
import java.util.List;

import com.redislabs.riot.transfer.TransferExecution.TransferExecutionBuilder;

import lombok.Builder;

public class Transfer<I, O> {

	private List<Flow<I, O>> flows;

	@Builder
	private Transfer(Flow<I, O> flow) {
		this.flows = new ArrayList<>();
		this.flows.add(flow);
	}

	public Transfer<I, O> add(Flow<I, O> flow) {
		this.flows.add(flow);
		return this;
	}

	public TransferExecution<I, O> execute() {
		TransferExecutionBuilder<I, O> builder = TransferExecution.<I, O>builder();
		flows.forEach(f -> builder.flow(f.execute()));
		return builder.build();
	}

}