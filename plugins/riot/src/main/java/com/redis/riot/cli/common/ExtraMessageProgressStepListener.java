package com.redis.riot.cli.common;

import java.util.List;
import java.util.function.Supplier;

import me.tongfei.progressbar.ProgressBarBuilder;

/**
 * Any reader whose progress is tracked by a progress bar.
 * 
 * @author Julien Ruaux
 * @since 3.1.2
 */
public class ExtraMessageProgressStepListener extends ProgressStepListener {

    private final Supplier<String> extraMessage;

    public ExtraMessageProgressStepListener(ProgressBarBuilder progressBarBuilder, Supplier<String> extraMessage) {
        super(progressBarBuilder);
        this.extraMessage = extraMessage;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void afterWrite(List items) {
        progressBar.setExtraMessage(extraMessage.get());
        super.afterWrite(items);
    }

}
