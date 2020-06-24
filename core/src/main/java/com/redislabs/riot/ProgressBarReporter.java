package com.redislabs.riot;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.springframework.util.Assert;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProgressBarReporter {

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Transfer<?, ?> transfer;
    private final ProgressBarOptions options;
    private ProgressBar bar;
    private ScheduledFuture<?> scheduledFuture;

    @Builder
    private ProgressBarReporter(Transfer<?, ?> transfer, ProgressBarOptions options) {
        Assert.notNull(transfer, "Transfer is required.");
        Assert.notNull(options, "Options are required.");
        this.transfer = transfer;
        this.options = options;
    }

    public void start() {
        if (options.isQuiet()) {
            return;
        }
        ProgressBarBuilder builder = new ProgressBarBuilder();
        if (options.getInitialMax() != null) {
            builder.setInitialMax(options.getInitialMax());
        }
        builder.setTaskName(options.getTaskName());
        builder.showSpeed();
        this.bar = builder.build();
        this.scheduledFuture = scheduler.scheduleAtFixedRate(this::update, 0, options.getRefreshInterval(), TimeUnit.MILLISECONDS);
    }

    private void update() {
        if (bar == null) {
            return;
        }
        bar.stepTo(transfer.getWriteCount());
    }

    public void stop() {
        if (options.isQuiet()) {
            return;
        }
        bar.stepTo(transfer.getWriteCount());
        scheduler.shutdown();
        scheduledFuture.cancel(true);
        this.bar.close();
    }


}
