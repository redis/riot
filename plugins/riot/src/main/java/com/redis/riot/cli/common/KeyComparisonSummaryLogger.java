package com.redis.riot.cli.common;

import java.io.PrintWriter;
import java.text.MessageFormat;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;

import com.redis.riot.core.replicate.KeyComparisonStatusCountItemWriter;
import com.redis.spring.batch.util.KeyComparison.Status;

public class KeyComparisonSummaryLogger extends StepExecutionListenerSupport {

    private static final String DETAIL_MSG = "    {0}:      {1}";

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
        out.println("Verification failed. Total keys: " + writer.getTotalCount());
        out.println(detail(Status.MISSING));
        out.println(detail(Status.TYPE));
        out.println(detail(Status.VALUE));
        out.println(detail(Status.TTL));
        out.println(detail(Status.OK));
        return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
    }

    private String detail(Status status) {
        return MessageFormat.format(DETAIL_MSG, status, writer.getCount(status));
    }

}
