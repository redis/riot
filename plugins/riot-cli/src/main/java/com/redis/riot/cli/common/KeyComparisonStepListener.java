package com.redis.riot.cli.common;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;

import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter;
import com.redis.spring.batch.writer.KeyComparisonCountItemWriter.Results;

public class KeyComparisonStepListener extends StepExecutionListenerSupport {

	private static final Logger log = Logger.getLogger(KeyComparisonStepListener.class.getName());

	private final KeyComparisonCountItemWriter writer;
	private final long sleep;

	public KeyComparisonStepListener(KeyComparisonCountItemWriter writer, long sleep) {
		this.writer = writer;
		this.sleep = sleep;
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
		try {
			Thread.sleep(sleep);
		} catch (InterruptedException e) {
			log.fine("Verification interrupted");
			Thread.currentThread().interrupt();
			return ExitStatus.STOPPED;
		}
		log.log(Level.WARNING, "Verification failed: OK={0} Missing={1} Values={2} TTLs={3} Types={4}",
				new Object[] { results.getCount(Status.OK), results.getCount(Status.MISSING),
						results.getCount(Status.VALUE), results.getCount(Status.TTL), results.getCount(Status.TYPE) });
		return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
	}
}