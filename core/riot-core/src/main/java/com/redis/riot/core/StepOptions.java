package com.redis.riot.core;

import java.time.Duration;

import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;

public class StepOptions {

    public static final SkipPolicy DEFAULT_SKIP_POLICY = new NeverSkipItemSkipPolicy();

    public static final int DEFAULT_SKIP_LIMIT = 0;

    public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;

    public static final Duration DEFAULT_SLEEP = Duration.ZERO;

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final int DEFAULT_THREADS = 1;

    private int threads = DEFAULT_THREADS;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private Duration sleep = DEFAULT_SLEEP;

    private boolean dryRun;

    private int skipLimit = DEFAULT_SKIP_LIMIT;

    private int retryLimit = DEFAULT_RETRY_LIMIT;

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public Duration getSleep() {
        return sleep;
    }

    public void setSleep(Duration sleep) {
        this.sleep = sleep;
    }

    public int getSkipLimit() {
        return skipLimit;
    }

    public void setSkipLimit(int skipLimit) {
        this.skipLimit = skipLimit;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }

}
