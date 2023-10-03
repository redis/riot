package com.redis.riot.cli;

import java.time.Duration;

import com.redis.riot.core.KeyComparisonMode;
import com.redis.riot.core.KeyComparisonOptions;
import com.redis.spring.batch.common.KeyComparisonItemReader;

import picocli.CommandLine.Option;

public class ReplicationCompareArgs {

    @Option(names = "--no-verify", description = "Disable comparing target against source after replication.")
    boolean noVerify;

    @Option(names = "--ttl-tolerance", description = "Max TTL offset in millis to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
    long ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE.toMillis();

    @Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
    boolean showDiffs;

    @Option(names = "--compare-mode", description = "Comparison mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
    KeyComparisonMode mode = KeyComparisonOptions.DEFAULT_MODE;

    public KeyComparisonOptions comparisonOptions() {
        KeyComparisonOptions options = new KeyComparisonOptions();
        options.setSkip(noVerify);
        options.setShowDiffs(showDiffs);
        options.setTtlTolerance(Duration.ofMillis(ttlTolerance));
        options.setMode(mode);
        return options;
    }

}
