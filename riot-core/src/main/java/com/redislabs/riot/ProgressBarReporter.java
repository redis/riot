package com.redislabs.riot;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.util.Assert;

import lombok.Builder;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

public class ProgressBarReporter {

    private final ProgressProvider progressProvider;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Integer initialMax;
    private final String taskName;
    private final boolean quiet;

    private ProgressBar bar;
    private ScheduledFuture<?> scheduledFuture;

    @Builder
    private ProgressBarReporter(ProgressProvider progressProvider, String taskName, Integer initialMax, boolean quiet) {
        Assert.notNull(progressProvider, "A progress provider is required.");
        Assert.notNull(taskName, "A task name is required.");
        this.progressProvider = progressProvider;
        this.taskName = taskName;
        this.initialMax = initialMax;
        this.quiet = quiet;
    }

    public void start() {
        if (quiet) {
            return;
        }
        ProgressBarBuilder builder = new ProgressBarBuilder();
        if (initialMax != null) {
            builder.setInitialMax(initialMax);
        }
        builder.setTaskName(taskName);
        builder.showSpeed();
        this.bar = builder.build();
        this.scheduledFuture = scheduler.scheduleAtFixedRate(this::update, 300, 300, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (quiet) {
            return;
        }
        bar.stepTo(progressProvider.getWriteCount());
        scheduler.shutdown();
        scheduledFuture.cancel(true);
        this.bar.close();
    }

    private void update() {
        if (bar == null) {
            return;
        }
        bar.stepTo(progressProvider.getWriteCount());
    }

}
