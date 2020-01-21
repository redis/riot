package com.redislabs.riot.transfer;

import java.util.ArrayList;
import java.util.List;

public class Transfer {

	private List<Flow> flows = new ArrayList<>();

	public TransferExecution execute() {
		TransferExecution execution = new TransferExecution();
		flows.forEach(f -> execution.flowExecution(f.execute()));
		return execution;
	}

	public void flow(Flow flow) {
		flows.add(flow);
	}

}