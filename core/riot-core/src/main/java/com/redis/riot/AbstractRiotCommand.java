package com.redis.riot;

import io.lettuce.core.RedisURI;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.util.Assert;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.util.concurrent.Callable;
import java.util.function.Function;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class AbstractRiotCommand extends HelpCommand implements Callable<Integer>, JobExecutionListener {

    @SuppressWarnings("unused")
    @ParentCommand
    private RiotApp app;

    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec spec;

    private ExecutionStrategy executionStrategy = ExecutionStrategy.SYNC;

    protected RedisOptions getRedisOptions() {
        return app.getRedisOptions();
    }

    protected String name(RedisURI redisURI) {
        if (redisURI.getSocket() != null) {
            return redisURI.getSocket();
        }
        if (redisURI.getSentinelMasterId() != null) {
            return redisURI.getSentinelMasterId();
        }
        return redisURI.getHost();
    }

    protected final Flow flow(Step... steps) {
        Assert.notNull(steps, "Steps are required");
        Assert.isTrue(steps.length > 0, "At least one step is required");
        FlowBuilder<SimpleFlow> flow = flow(spec.name()+"-flow");
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
        if (executionStrategy == ExecutionStrategy.ASYNC) {
            return awaitRunning(execution);
        }
        return exitCode(execution);
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

        private final Function<JobFactory, JobLauncher> launcher;

        ExecutionStrategy(Function<JobFactory, JobLauncher> launcher) {
            this.launcher = launcher;
        }
    }

    public JobExecution execute() throws Exception {
        JobFactory jobFactory = new JobFactory();
        jobFactory.afterPropertiesSet();
        JobBuilder builder = jobFactory.getJobBuilderFactory().get(spec.name());
        Job job = builder.listener(this).start(flow(jobFactory.getStepBuilderFactory())).build().build();
        return executionStrategy.launcher.apply(jobFactory).run(job, new JobParameters());
    }

    protected abstract Flow flow(StepBuilderFactory stepBuilderFactory) throws Exception;

    @Override
    public void afterJob(JobExecution jobExecution) {
        getRedisOptions().shutdown();
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        // do nothing
    }

}
