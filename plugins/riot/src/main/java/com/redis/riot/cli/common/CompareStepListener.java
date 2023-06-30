package com.redis.riot.cli.common;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;

import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;

public class CompareStepListener extends StepExecutionListenerSupport {

	private static final Logger log = Logger.getLogger(CompareStepListener.class.getName());

	private final KeyComparisonCountItemWriter writer;

	public CompareStepListener(KeyComparisonCountItemWriter writer) {
		this.writer = writer;
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (stepExecution.getStatus().isUnsuccessful()) {
			return null;
		}
		Results results = writer.getResults();
		if (results.getTotalCount() == results.getCount(Status.OK)) {
			log.info("Verification completed - all OK");
			return ExitStatus.COMPLETED;
		}
		log.log(Level.SEVERE, "Verification failed: {0} ok, {1} missing, {4} type, {2} value, {3} ttl",
				new Object[] { results.getCount(Status.OK), results.getCount(Status.MISSING),
						results.getCount(Status.VALUE), results.getCount(Status.TTL), results.getCount(Status.TYPE) });
		return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
	}
}