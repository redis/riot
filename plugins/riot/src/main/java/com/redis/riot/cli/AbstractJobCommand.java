package com.redis.riot.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.riot.core.AbstractJobExecutable;
import com.redis.riot.core.Executable;
import com.redis.riot.core.StepBuilder;

import io.lettuce.core.AbstractRedisClient;
import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBarBuilder;
import picocli.CommandLine.ArgGroup;

abstract class AbstractJobCommand extends AbstractCommand {

    @ArgGroup(exclusive = false, heading = "Execution options%n")
    StepArgs stepArgs = new StepArgs();

    @ArgGroup(exclusive = false)
    ProgressArgs progressArgs = new ProgressArgs();

    @Override
    protected Executable getExecutable() {
        AbstractJobExecutable executable = getJobExecutable();
        executable.setStepOptions(stepArgs.stepOptions());
        executable.addStepConsumer(this::configure);
        return executable;
    }

    protected <I, O> void configure(StepBuilder<I, O> step) {
        ProgressBarBuilder progressBar = progressBar();
        progressBar.setInitialMax(size(step));
        progressBar.setTaskName(taskName(step));
        ProgressStepListener<O> listener = new ProgressStepListener<>(progressBar);
        step.addExecutionListener(listener);
        step.addWriteListener(listener);
    }

    private ProgressBarBuilder progressBar() {
        ProgressBarBuilder progressBar = new ProgressBarBuilder();
        progressBar.setStyle(progressArgs.style());
        progressBar.setUpdateIntervalMillis(progressArgs.updateInterval);
        progressBar.showSpeed();
        if (progressArgs.isLog()) {
            Logger logger = LoggerFactory.getLogger(getClass());
            progressBar.setConsumer(new DelegatingProgressBarConsumer(logger::info));
        }
        return progressBar;
    }

    protected abstract String taskName(StepBuilder<?, ?> step);

    protected abstract long size(StepBuilder<?, ?> step);

    protected RedisArgs redisArgs() {
        return parent.redisArgs;
    }

    protected AbstractRedisClient redisClient() {
        return parent.redisArgs.client();
    }

    protected abstract AbstractJobExecutable getJobExecutable();

}
