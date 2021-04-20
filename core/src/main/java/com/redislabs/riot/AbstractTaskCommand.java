package com.redislabs.riot;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command
public abstract class AbstractTaskCommand extends RiotCommand {

    @SuppressWarnings("unused")
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    protected JobFactory jobFactory;

    protected final Flow flow(Step... steps) {
        Assert.notNull(steps, "Steps are required.");
        Assert.isTrue(steps.length > 0, "At least one step is required.");
        FlowBuilder<SimpleFlow> flow = flow(spec.name());
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
    public void afterPropertiesSet() throws Exception {
        jobFactory = new JobFactory();
        jobFactory.afterPropertiesSet();
        super.afterPropertiesSet();
    }

    @Override
    protected int execute() throws Exception {
        Job job = job();
        JobParameters parameters = new JobParameters();
        if (isExecuteAsync()) {
            JobExecution execution = jobFactory.getAsyncLauncher().run(job(), parameters);
            while (!execution.isRunning()) {
                Thread.sleep(10);
            }
        } else {
            JobExecution execution = jobFactory.getSyncLauncher().run(job, parameters);
            for (StepExecution stepExecution : execution.getStepExecutions()) {
                if (stepExecution.getExitStatus().compareTo(ExitStatus.FAILED) >= 0) {
                    return 1;
                }
            }
        }
        return 0;
    }

    private Job job() throws Exception {
        JobBuilder builder = jobFactory.job(ClassUtils.getShortName(getClass()));
        if (isExecuteAsync()) {
            builder.listener(new JobExecutionListenerSupport() {
                @Override
                public void afterJob(JobExecution jobExecution) {
                    shutdown();
                    super.afterJob(jobExecution);
                }
            });
        }
        return builder.start(flow()).build().build();
    }

    protected abstract Flow flow() throws Exception;

}
