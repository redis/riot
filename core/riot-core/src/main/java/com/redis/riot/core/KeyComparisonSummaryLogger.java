package com.redis.riot.core;

import java.util.List;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;

import com.redis.spring.batch.common.KeyComparison.Status;

public class KeyComparisonSummaryLogger extends StepExecutionListenerSupport {

    private static final String MESSAGE = "Verification failed: %,d missing, %,d type, %,d value, %,d TTL";

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
