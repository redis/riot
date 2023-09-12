package com.redis.riot.core;

import java.time.Duration;

public class StepOptions {

    public static final RiotSkipPolicy DEFAULT_SKIP_POLICY = RiotSkipPolicy.LIMIT;

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final int DEFAULT_THREADS = 1;

    public static final int DEFAULT_SKIP_LIMIT = 3;

    public static final int DEFAULT_RETRY_LIMIT = 1;

    public static final int DEFAULT_PROGRESS_UPDATE_INTERVAL = 1000;

    private int threads = DEFAULT_THREADS;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private Duration sleep;

    private boolean dryRun;

    private boolean faultTolerance;

    private RiotSkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;

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

    public boolean isFaultTolerance() {
        return faultTolerance;
    }

    public void setFaultTolerance(boolean faultTolerance) {
        this.faultTolerance = faultTolerance;
    }

    public RiotSkipPolicy getSkipPolicy() {
        return skipPolicy;
    }

    public void setSkipPolicy(RiotSkipPolicy skipPolicy) {
        this.skipPolicy = skipPolicy;
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
