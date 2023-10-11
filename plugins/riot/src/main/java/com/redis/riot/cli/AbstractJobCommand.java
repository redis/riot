package com.redis.riot.cli;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.util.ClassUtils;

import com.redis.riot.cli.ProgressArgs.ProgressStyle;
import com.redis.riot.core.AbstractJobRunnable;
import com.redis.riot.core.EvaluationContextOptions;
import com.redis.riot.core.RiotStep;

import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBarBuilder;
import picocli.CommandLine.ArgGroup;

abstract class AbstractJobCommand extends AbstractCommand {

    @ArgGroup(exclusive = false, heading = "Execution options%n")
    StepArgs stepArgs = new StepArgs();

    @ArgGroup(exclusive = false)
    ProgressArgs progressArgs = new ProgressArgs();

    @ArgGroup(exclusive = false)
    EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

    @Override
    protected AbstractJobRunnable executable() {
        AbstractJobRunnable executable = getJobExecutable();
        executable.setStepOptions(stepArgs.stepOptions());
        executable.setEvaluationContextOptions(evaluationContextOptions());
        executable.setStepConfigurer(this::configureStep);
        return executable;
    }

    protected EvaluationContextOptions evaluationContextOptions() {
        return evaluationContextArgs.evaluationContextOptions();
    }

    private void configureStep(RiotStep<?, ?> step) {
        if (progressArgs.style == ProgressStyle.NONE) {
            return;
        }
        ProgressBarBuilder progressBar = new ProgressBarBuilder();
        progressBar.setTaskName(taskName(step));
        progressBar.setStyle(progressArgs.progressBarStyle());
        progressBar.setUpdateIntervalMillis(progressArgs.updateInterval);
        progressBar.showSpeed();
        if (progressArgs.style == ProgressStyle.LOG) {
            Logger logger = LoggerFactory.getLogger(getClass());
            progressBar.setConsumer(new DelegatingProgressBarConsumer(logger::info));
        }
        ProgressStepExecutionListener listener = new ProgressStepExecutionListener(progressBar);
        listener.setExtraMessageSupplier(extraMessageSupplier(step));
        listener.setInitialMaxSupplier(initialMaxSupplier(step));
        step.setConfigurer(s -> {
            s.listener((StepExecutionListener) listener);
            s.listener((ItemWriteListener<?>) listener);
        });
    }

    protected String taskName(RiotStep<?, ?> step) {
        return ClassUtils.getShortName(getClass());
    }

    protected Supplier<String> extraMessageSupplier(RiotStep<?, ?> step) {
        return () -> ProgressStepExecutionListener.EMPTY_STRING;
    }

    protected LongSupplier initialMaxSupplier(RiotStep<?, ?> step) {
        return () -> ProgressStepExecutionListener.UNKNOWN_SIZE;
    }

    protected abstract AbstractJobRunnable getJobExecutable();

}
