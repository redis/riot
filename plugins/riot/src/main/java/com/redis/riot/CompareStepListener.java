package com.redis.riot;

import java.util.Collection;
import java.util.stream.Collectors;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import com.redis.riot.CompareStatusItemWriter.StatusCount;

public class CompareStepListener implements StepExecutionListener {

	private static final String STATUS_FORMAT = "%s: %,d";

	private final CompareStatusItemWriter<byte[]> writer;

	public CompareStepListener(CompareStatusItemWriter<byte[]> writer) {
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
		return new ExitStatus(ExitStatus.FAILED.getExitCode(), exitDescription());
	}

	private String exitDescription() {
		return String.format("Verification failed (%s)", toString(writer.getMismatches()));
	}

	private String toString(Collection<StatusCount> counts) {
		return String.join(", ", counts.stream().map(CompareStepListener::toString).collect(Collectors.toList()));
	}

	public static String toString(StatusCount count) {
		return String.format(STATUS_FORMAT, count.getStatus().name().toLowerCase(), count.getCount());
	}

}
