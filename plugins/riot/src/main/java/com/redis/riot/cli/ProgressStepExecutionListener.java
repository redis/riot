package com.redis.riot.cli;

import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.listener.ItemListenerSupport;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

/**
 * Listener tracking writer or step progress with by a progress bar.
 * 
 * @author Julien Ruaux
 * @since 3.1.2
 */
@SuppressWarnings("rawtypes")
public class ProgressStepExecutionListener extends ItemListenerSupport implements StepExecutionListener {

    public static final long UNKNOWN_SIZE = -1;

    public static final String EMPTY_STRING = "";

    private final ProgressBarBuilder builder;

    private LongSupplier initialMaxSupplier = () -> UNKNOWN_SIZE;

    private Supplier<String> extraMessageSupplier = () -> EMPTY_STRING;

    private ProgressBar progressBar;

    public ProgressStepExecutionListener(ProgressBarBuilder builder) {
        this.builder = builder;
    }

    public void setInitialMaxSupplier(LongSupplier supplier) {
        this.initialMaxSupplier = supplier;
    }

    public void setExtraMessageSupplier(Supplier<String> supplier) {
        this.extraMessageSupplier = supplier;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        progressBar = builder.build();
        progressBar.maxHint(initialMaxSupplier.getAsLong());
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
    public void afterWrite(List items) {
        progressBar.stepBy(items.size());
        progressBar.setExtraMessage(extraMessageSupplier.get());
    }

    @Override
    public void onWriteError(Exception exception, List items) {
        progressBar.stepBy(items.size());

    }

}
