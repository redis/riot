package com.redislabs.riot;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;

import java.util.function.Function;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@CommandLine.Command
public abstract class AbstractTaskCommand extends RiotCommand {

    private ExecutionStrategy executionStrategy = ExecutionStrategy.SYNC;

    protected final Flow flow(Step... steps) {
        Assert.notNull(steps, "Steps are required");
        Assert.isTrue(steps.length > 0, "At least one step is required");
        FlowBuilder<SimpleFlow> flow = flow(ClassUtils.getShortName(getClass()));
        flow.start(steps[0]);
        for (int index = 1; index < steps.length; index++) {
            flow.next(steps[index]);
        }
        return flow.build();
    }

    protected final FlowBuilder<SimpleFlow> flow(String name) {
        return new FlowBuilder<>(name + "-flow");
    }

    @Override
    public Integer call() throws Exception {
        JobExecution execution = execute();
        switch (executionStrategy) {
            case ASYNC:
                return awaitRunning(execution);
            default:
                return exitCode(execution);
        }
    }

    private int awaitRunning(JobExecution execution) {
        while (!execution.isRunning()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.debug("Interrupted");
                return 1;
            }
        }
        return 0;
    }

    private int exitCode(JobExecution execution) {
        for (StepExecution stepExecution : execution.getStepExecutions()) {
            if (stepExecution.getExitStatus().compareTo(ExitStatus.FAILED) >= 0) {
                return 1;
            }
        }
        return 0;
    }

    public enum ExecutionStrategy {

        SYNC(JobFactory::getSyncLauncher), ASYNC(JobFactory::getAsyncLauncher);

        private Function<JobFactory, JobLauncher> launcher;

        ExecutionStrategy(Function<JobFactory, JobLauncher> launcher) {
            this.launcher = launcher;
        }
    }

    private JobExecution execute() throws Exception {
        JobFactory jobFactory = new JobFactory();
        jobFactory.afterPropertiesSet();
        JobBuilder builder = jobFactory.getJobBuilderFactory().get(ClassUtils.getShortName(getClass()));
        Job job = builder.start(flow(jobFactory.getStepBuilderFactory())).build().build();
        return executionStrategy.launcher.apply(jobFactory).run(job, new JobParameters());
    }

    protected abstract Flow flow(StepBuilderFactory stepBuilderFactory) throws Exception;

}
