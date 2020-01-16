package com.redislabs.riot.transfer;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class Transfer {

	@Getter
	private List<Flow> flows = new ArrayList<>();

	public TransferExecution execute() {
		List<FlowExecution> flowExecutions = new ArrayList<>();
		flows.forEach(f -> flowExecutions.add(f.execute()));
		return TransferExecution.builder().flowExecutions(flowExecutions).build();
	}

}