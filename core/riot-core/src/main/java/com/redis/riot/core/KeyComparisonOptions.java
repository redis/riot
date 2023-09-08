package com.redis.riot.core;

import java.time.Duration;

import com.redis.spring.batch.util.KeyComparisonItemReader;

public class KeyComparisonOptions {

    private boolean noVerify;

    private boolean showDiff;

    private Duration ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE;

    public boolean isNoVerify() {
        return noVerify;
    }

    public void setNoVerify(boolean noVerify) {
        this.noVerify = noVerify;
    }

    public boolean isShowDiff() {
        return showDiff;
    }

    public void setShowDiff(boolean showDiff) {
        this.showDiff = showDiff;
    }

    public Duration getTtlTolerance() {
        return ttlTolerance;
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance;
    }

}
