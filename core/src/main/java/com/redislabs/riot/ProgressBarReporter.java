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
public class ProgressBarReporter implements Transfer.Listener {

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Transfer<?, ?> transfer;
    private final Integer initialMax;
    private final String taskName;
    private final boolean quiet;

    private ProgressBar bar;
    private ScheduledFuture<?> scheduledFuture;

    @Builder
    private ProgressBarReporter(Transfer<?, ?> transfer, String taskName, Integer initialMax, boolean quiet) {
        Assert.notNull(transfer, "A Transfer instance is required.");
        Assert.notNull(taskName, "A task name is required.");
        this.transfer = transfer;
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
        bar.stepTo(transfer.getWriteCount());
        scheduler.shutdown();
        scheduledFuture.cancel(true);
        this.bar.close();
    }

    private void update() {
        if (bar == null) {
            return;
        }
        bar.stepTo(transfer.getWriteCount());
    }


    @Override
    public void onOpen() {
        start();
    }

    @Override
    public void onUpdate(long writeCount) {
        // do nothing
    }

    @Override
    public void onClose() {
        stop();
    }
}
