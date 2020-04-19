package com.redislabs.riot.cli;

import com.redislabs.riot.transfer.Metrics;
import com.redislabs.riot.transfer.MetricsProvider;
import com.redislabs.riot.transfer.Transfer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProgressBarReporter implements Runnable, Transfer.Listener {

    private ScheduledExecutorService executor;
    private MetricsProvider metricsProvider;
    private Long period = null;
    private TimeUnit timeUnit;
    private ScheduledFuture<?> scheduledFuture;
    private ProgressBarBuilder builder = new ProgressBarBuilder();
    private ProgressBar bar;

    @Builder
    private ProgressBarReporter(Long initialMax, String taskName, String unitName, MetricsProvider metricsProvider, Long period, TimeUnit timeUnit) {
        if (initialMax != null) {
            builder.setInitialMax(initialMax);
        }
        builder.setTaskName(taskName);
        if (unitName != null) {
            builder.setUnit(" " + unitName + "s", 1);
        }
        if (metricsProvider != null && period != null && timeUnit != null) {
            this.metricsProvider = metricsProvider;
            this.period = period;
            this.timeUnit = timeUnit;
            this.executor = Executors.newSingleThreadScheduledExecutor();
            this.scheduledFuture = executor.scheduleAtFixedRate(this, 0, period, timeUnit);
        }
    }

    @Override
    public void onOpen() {
        this.bar = builder.build();
    }

    @Override
    public void onClose() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        if (executor != null) {
            executor.shutdown();
        }
        run();
        this.bar.close();
    }

    @Override
    public void run() {
        if (bar == null) {
            return;
        }
        Metrics metrics = metricsProvider.getMetrics();
        bar.stepTo(metrics.getWrites());
        int runningThreads = metrics.getRunningThreads();
        if (runningThreads > 1) {
            bar.setExtraMessage(" (" + runningThreads + " threads)");
        }
    }
}
