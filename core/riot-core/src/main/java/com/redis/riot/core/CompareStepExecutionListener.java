package com.redis.riot.core;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;

import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;

public class CompareStepExecutionListener extends StepExecutionListenerSupport {

	private final Logger log;
	private final KeyComparisonCountItemWriter writer;

	public CompareStepExecutionListener(KeyComparisonCountItemWriter writer, Logger logger) {
		this.writer = writer;
		this.log = logger;
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (stepExecution.getStatus().isUnsuccessful()) {
			return null;
		}
		Results results = writer.getResults();
		if (results.getTotalCount() == results.getCount(Status.OK)) {
			log.info("Verification completed: all OK");
			return ExitStatus.COMPLETED;
		}
		severe("Verification failed:\n    OK:      {1}\n    Missing: {2}\n    Type:    {3}\n    Value:   {4}\n    TTL:     {5}\n    Total:   {0}",
				results.getTotalCount(), results.getCount(Status.OK), results.getCount(Status.MISSING),
				results.getCount(Status.TYPE), results.getCount(Status.VALUE), results.getCount(Status.TTL));
		return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
	}

	private void severe(String msg, Object... params) {
		log.log(Level.SEVERE, msg, params);
	}

}