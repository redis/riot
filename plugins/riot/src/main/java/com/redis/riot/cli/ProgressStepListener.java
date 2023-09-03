package com.redis.riot.cli;

import java.util.List;
import java.util.function.Supplier;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

/**
 * Listener tracking writer or step progress with by a progress bar.
 * 
 * @author Julien Ruaux
 * @since 3.1.2
 */
public class ProgressStepListener<O> implements ItemWriteListener<O>, StepExecutionListener {

    private final ProgressBarBuilder progressBarBuilder;

    protected ProgressBar progressBar;

    public ProgressStepListener(ProgressBarBuilder progressBarBuilder) {
        this.progressBarBuilder = progressBarBuilder;
    }

    public ProgressBarBuilder getProgressBarBuilder() {
        return progressBarBuilder;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        progressBar = progressBarBuilder.build();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (!stepExecution.getStatus().isUnsuccessful()) {
            progressBar.stepTo(progressBar.getMax());
        }
        progressBar.close();
        return stepExecution.getExitStatus();
    }

    @Override
    public void beforeWrite(List<? extends O> items) {
        // Do nothing
    }

    @Override
    public void afterWrite(List<? extends O> items) {
        progressBar.stepBy(items.size());
    }

    @Override
    public void onWriteError(Exception exception, List<? extends O> items) {
        progressBar.stepBy(items.size());
    }

    public ExtraMessageProgressStepListener<O> extraMessage(Supplier<String> extraMessage) {
        return new ExtraMessageProgressStepListener<>(progressBarBuilder, extraMessage);
    }

    private static class ExtraMessageProgressStepListener<O> extends ProgressStepListener<O> {

        private final Supplier<String> extraMessage;

        public ExtraMessageProgressStepListener(ProgressBarBuilder progressBarBuilder, Supplier<String> extraMessage) {
            super(progressBarBuilder);
            this.extraMessage = extraMessage;
        }

        @Override
        public void afterWrite(List<? extends O> items) {
            progressBar.setExtraMessage(extraMessage.get());
            super.afterWrite(items);
        }

    }

}
