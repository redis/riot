package com.redis.riot.redis;

import java.util.List;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import com.redis.spring.batch.reader.KeyComparison.Status;

public class KeyComparisonSummaryLogger implements StepExecutionListener {

	private static final String MESSAGE = "Verification failed. Missing: %,d, Type: %,d, Value: %,d, TTL: %,d";

	private final KeyComparisonStatusCountItemWriter writer;

	public KeyComparisonSummaryLogger(KeyComparisonStatusCountItemWriter writer) {
		this.writer = writer;
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (stepExecution.getStatus().isUnsuccessful()) {
			return null;
		}
		if (writer.getTotal() == writer.getOK()) {
			return ExitStatus.COMPLETED;
		}
		List<Long> counts = writer.getCounts(Status.MISSING, Status.TYPE, Status.VALUE, Status.TTL);
		return new ExitStatus(ExitStatus.FAILED.getExitCode(), String.format(MESSAGE, counts.toArray()));
	}

}
