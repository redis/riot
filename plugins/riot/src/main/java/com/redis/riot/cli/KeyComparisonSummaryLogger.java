package com.redis.riot.cli;

import java.io.PrintWriter;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;

import com.redis.riot.core.KeyComparisonStatusCountItemWriter;
import com.redis.spring.batch.util.KeyComparison.Status;

public class KeyComparisonSummaryLogger extends StepExecutionListenerSupport {

    private static final String DETAIL_MSG = "    %s:      %,d";

    private final PrintWriter out;

    private final KeyComparisonStatusCountItemWriter writer;

    public KeyComparisonSummaryLogger(KeyComparisonStatusCountItemWriter writer, PrintWriter out) {
        this.writer = writer;
        this.out = out;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (stepExecution.getStatus().isUnsuccessful()) {
            return null;
        }
        if (writer.getTotalCount() == writer.getCount(Status.OK)) {
            out.println("Verification completed: all OK");
            return ExitStatus.COMPLETED;
        }
        out.format("Verification failed. Total keys: %,d", writer.getTotalCount());
        detail(Status.MISSING);
        detail(Status.TYPE);
        detail(Status.VALUE);
        detail(Status.TTL);
        detail(Status.OK);
        return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
    }

    private void detail(Status status) {
        out.format(DETAIL_MSG, status, writer.getCount(status));
    }

}
