package com.redislabs.recharge.batch;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MeteredJobExecutionListener implements JobExecutionListener {

	@Autowired
	private MeteringProvider metering;

	@Override
	public void beforeJob(JobExecution jobExecution) {
		metering.startLongTaskTimer(jobExecution);
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		metering.stop(jobExecution);
	}

}