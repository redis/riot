package com.redislabs.riot.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.item.redis.support.KeyComparisonItemWriter;
import org.springframework.batch.item.redis.support.KeyComparisonResults;

import java.util.List;

@Slf4j
public class VerificationStepExecutionListener extends StepExecutionListenerSupport {

    private final KeyComparisonItemWriter<String> writer;

    public VerificationStepExecutionListener(KeyComparisonItemWriter<String> writer) {
        this.writer = writer;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (writer.getResults().isOk()) {
            return super.afterStep(stepExecution);
        }
        log.warn("Verification failed");
        KeyComparisonResults<String> results = writer.getResults();
        printDiffs(results.getLeft(), "missing keys");
        printDiffs(results.getRight(), "extraneous keys");
        printDiffs(results.getValue(), "mismatched values");
        printDiffs(results.getTtl(), "mismatched TTLs");
        return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
    }

    private void printDiffs(List<String> diffs, String messagePreamble) {
        if (diffs.isEmpty()) {
            return;
        }
        log.info("{} " + messagePreamble + ": {}", diffs.size(), diffs.subList(0, Math.min(10, diffs.size())));
    }
}