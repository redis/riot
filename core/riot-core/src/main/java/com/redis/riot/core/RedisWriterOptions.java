package com.redis.riot.core;

import java.time.Duration;

import com.redis.spring.batch.common.AbstractOperationExecutor;
import com.redis.spring.batch.writer.AbstractOperationItemWriter;

public class RedisWriterOptions {

    public static final Duration DEFAULT_WAIT_TIMEOUT = AbstractOperationItemWriter.DEFAULT_WAIT_TIMEOUT;

    public static final int DEFAULT_POOL_SIZE = AbstractOperationExecutor.DEFAULT_POOL_SIZE;

    private boolean multiExec;

    private int waitReplicas;

    private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;

    private int poolSize = DEFAULT_POOL_SIZE;

    private boolean merge;

    public boolean isMerge() {
        return merge;
    }

    public void setMerge(boolean merge) {
        this.merge = merge;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int size) {
        this.poolSize = size;
    }

    public boolean isMultiExec() {
        return multiExec;
    }

    public void setMultiExec(boolean enable) {
        this.multiExec = enable;
    }

    public int getWaitReplicas() {
        return waitReplicas;
    }

    public void setWaitReplicas(int replicas) {
        this.waitReplicas = replicas;
    }

    public Duration getWaitTimeout() {
        return waitTimeout;
    }

    public void setWaitTimeout(Duration timeout) {
        this.waitTimeout = timeout;
    }

}
