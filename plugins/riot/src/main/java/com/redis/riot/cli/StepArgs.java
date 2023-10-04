package com.redis.riot.cli;

import java.time.Duration;

import com.redis.riot.core.StepOptions;

import picocli.CommandLine.Option;

public class StepArgs {

    @Option(names = "--sleep", description = "Duration in ms to sleep after each batch write (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
    long sleep;

    @Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int threads = StepOptions.DEFAULT_THREADS;

    @Option(names = { "-b",
            "--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    int chunkSize = StepOptions.DEFAULT_CHUNK_SIZE;

    @Option(names = "--dry-run", description = "Enable dummy writes.")
    boolean dryRun;

    @Option(names = "--ft", description = "Enable step fault-tolerance. Use in conjunction with retry and skip limit/policy.")
    boolean faultTolerance;

    @Option(names = "--skip-limit", description = "LIMIT skip policy: max number of failed items before considering the transfer has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int skipLimit = StepOptions.DEFAULT_SKIP_LIMIT;

    @Option(names = "--retry-limit", description = "Maximum number of times to try failed items. 0 and 1 both translate to no retry. (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int retryLimit = StepOptions.DEFAULT_RETRY_LIMIT;

    public StepOptions stepOptions() {
        StepOptions options = new StepOptions();
        options.setChunkSize(chunkSize);
        options.setDryRun(dryRun);
        options.setRetryLimit(retryLimit);
        options.setSkipLimit(skipLimit);
        options.setSleep(Duration.ofMillis(sleep));
        options.setThreads(threads);
        return options;
    }

}
