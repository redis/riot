package com.redis.riot.cli;

import com.redis.riot.core.AbstractJobExecutable;
import com.redis.riot.core.Executable;
import com.redis.riot.core.StepBuilder;

import io.lettuce.core.AbstractRedisClient;
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
        ProgressBarBuilder progressBar = progressArgs.progressBar();
        progressBar.setInitialMax(size(step));
        progressBar.setTaskName(taskName(step));
        ProgressStepListener<O> listener = new ProgressStepListener<>(progressBar);
        step.addExecutionListener(listener);
        step.addWriteListener(listener);
    }

    protected abstract String taskName(StepBuilder<?, ?> step);

    protected abstract long size(StepBuilder<?, ?> step);

    protected RedisArgs redisArgs() {
        return parent.getRedisArgs();
    }

    protected AbstractRedisClient redisClient() {
        return parent.getRedisArgs().client();
    }

    protected abstract AbstractJobExecutable getJobExecutable();

}
