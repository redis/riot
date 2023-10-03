package com.redis.riot.core;

import java.io.PrintWriter;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;

import com.redis.spring.batch.common.KeyComparison.Status;

public class KeyComparisonSummaryLogger extends StepExecutionListenerSupport {

    private static final String DETAIL_MSG = "  %-7s %,10d";

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
        if (writer.getTotal() == writer.getCount(Status.OK)) {
            out.println("Verification completed: all OK");
            return ExitStatus.COMPLETED;
        }
        out.println("Verification failed:");
        detail(Status.MISSING);
        detail(Status.TYPE);
        detail(Status.VALUE);
        detail(Status.TTL);
        detail(Status.OK);
        return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
    }

    private void detail(Status status) {
        out.println(String.format(DETAIL_MSG, status.name().toLowerCase(), writer.getCount(status)));
    }

}
