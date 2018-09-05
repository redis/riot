package com.redislabs.recharge.batch;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MeteredStepExecutionListener implements StepExecutionListener {

	@Autowired
	private MeteringProvider metering;

	@Override
	public void beforeStep(StepExecution stepExecution) {
		metering.startLongTaskTimer(stepExecution);
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		metering.stop(stepExecution);
		return null;
	}

}
