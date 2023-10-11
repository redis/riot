package com.redis.riot.core;

import java.time.Duration;

import com.redis.spring.batch.reader.KeyComparisonValueReader;

public class KeyComparisonOptions {

    public static final KeyComparisonMode DEFAULT_MODE = KeyComparisonMode.QUICK;

    private boolean skip;

    private boolean showDiffs;

    private KeyComparisonMode mode = DEFAULT_MODE;

    private Duration ttlTolerance = KeyComparisonValueReader.DEFAULT_TTL_TOLERANCE;

    public KeyComparisonMode getMode() {
        return mode;
    }

    public void setMode(KeyComparisonMode mode) {
        this.mode = mode;
    }

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    public boolean isShowDiffs() {
        return showDiffs;
    }

    public void setShowDiffs(boolean showDiff) {
        this.showDiffs = showDiff;
    }

    public Duration getTtlTolerance() {
        return ttlTolerance;
    }

    public void setTtlTolerance(Duration ttlTolerance) {
        this.ttlTolerance = ttlTolerance;
    }

}
