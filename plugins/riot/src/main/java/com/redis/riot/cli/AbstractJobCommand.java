package com.redis.riot.cli;

import com.redis.riot.core.AbstractJobExecutable;
import com.redis.riot.core.Executable;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.ArgGroup;

abstract class AbstractJobCommand extends AbstractLoggingCommand<Main> {

    // @Option(names = "--progress", description = "Style of progress bar: ${COMPLETION-CANDIDATES} (default:
    // ${DEFAULT-VALUE}),", paramLabel = "<style>")
    // private ProgressStyle progressStyle = ProgressStyle.ASCII;
    //
    // @Option(names = "--progress-interval", description = "Progress update interval in millis (default: ${DEFAULT-VALUE}).",
    // paramLabel = "<ms>", hidden = true)
    // private int progressUpdateInterval = 300;

    //
    // private ProgressStepListener<O> progressStepListener() {
    // ProgressBarBuilder pbb = new ProgressBarBuilder();
    // pbb.setInitialMax(initialMax());
    // pbb.setTaskName(task);
    // pbb.setStyle(progressBarStyle());
    // pbb.setUpdateIntervalMillis(options.getProgressUpdateInterval());
    // pbb.showSpeed();
    // if (progressStyle() == ProgressStyle.LOG) {
    // pbb.setConsumer(new DelegatingProgressBarConsumer(logger));
    // }
    // if (extraMessage == null) {
    // return new ProgressStepListener<>(pbb);
    // }
    // return new ProgressWithExtraMessageStepListener<>(pbb, extraMessage);
    // }
    //
    // private ProgressStyle progressStyle() {
    // return progressStyle.orElse(options.getProgressStyle());
    // }
    //
    // private long initialMax() {
    // if (reader instanceof FakerItemReader) {
    // return ((FakerItemReader) reader).size();
    // }
    // return Utils.getItemReaderSize(reader);
    // }
    //
    // private ProgressBarStyle progressBarStyle() {
    // switch (progressStyle()) {
    // case BAR:
    // return ProgressBarStyle.COLORFUL_UNICODE_BAR;
    // case BLOCK:
    // return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
    // default:
    // return ProgressBarStyle.ASCII;
    // }
    // }

    @ArgGroup(exclusive = false, heading = "Execution options%n")
    private StepArgs stepArgs = new StepArgs();

    protected RedisArgs redisArgs() {
        return parent.getRedisArgs();
    }

    protected AbstractRedisClient redisClient() {
        return parent.getRedisArgs().client();
    }

    @Override
    protected Executable getExecutable() {
        AbstractJobExecutable executable = getJobExecutable();
        executable.setStepOptions(stepArgs.stepOptions());
        return executable;
    }

    protected abstract AbstractJobExecutable getJobExecutable();

}
