package com.redislabs.riot;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepListenerSupport;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Slf4j
@Builder
public class ProgressMonitor<S, T> extends StepListenerSupport<S, T> {

    private final String taskName;
    private final Integer updateIntervalMillis;
    private final Supplier<Long> initialMax;
    private final Supplier<String> extraMessage;
    private ProgressBar progressBar;
    private AtomicLong count;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        super.beforeStep(stepExecution);
        count = new AtomicLong();
        ProgressBarBuilder builder = new ProgressBarBuilder();
        if (initialMax != null) {
            Long initialMaxValue = initialMax.get();
            if (initialMaxValue != null) {
                builder.setInitialMax(initialMaxValue);
            }
        }
        if (updateIntervalMillis != null) {
            builder.setUpdateIntervalMillis(updateIntervalMillis);
        }
        builder.setTaskName(taskName);
        builder.showSpeed();
        this.progressBar = builder.build();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        progressBar.close();
        return super.afterStep(stepExecution);
    }

    @Override
    public void afterWrite(List<? extends T> items) {
        progressBar.stepTo(count.addAndGet(items.size()));
        if (extraMessage != null) {
            progressBar.setExtraMessage(extraMessage.get());
        }
        super.afterWrite(items);
    }
}