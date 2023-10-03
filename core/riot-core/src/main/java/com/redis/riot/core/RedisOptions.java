package com.redis.riot.core;

import java.time.Duration;

public class RedisOptions {

    private RedisUriOptions uriOptions = new RedisUriOptions();

    private RedisClientOptions clientOptions = new RedisClientOptions();

    private boolean cluster;

    private Duration metricsStep;

    public RedisUriOptions getUriOptions() {
        return uriOptions;
    }

    public void setUriOptions(RedisUriOptions uriOptions) {
        this.uriOptions = uriOptions;
    }

    public RedisClientOptions getClientOptions() {
        return clientOptions;
    }

    public void setClientOptions(RedisClientOptions clientOptions) {
        this.clientOptions = clientOptions;
    }

    public boolean isCluster() {
        return cluster;
    }

    public void setCluster(boolean cluster) {
        this.cluster = cluster;
    }

    public Duration getMetricsStep() {
        return metricsStep;
    }

    public void setMetricsStep(Duration metricsStep) {
        this.metricsStep = metricsStep;
    }

}
