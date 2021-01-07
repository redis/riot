package com.redislabs.riot;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;

@CommandLine.Command
public abstract class AbstractTaskCommand extends RiotCommand {

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    private JobFactory factory;

    protected final Flow flow(Step... steps) {
        Assert.notNull(steps, "Steps are required.");
        Assert.isTrue(steps.length > 0, "At least one step is required.");
        FlowBuilder<SimpleFlow> flow = flowBuilder(spec.name());
        flow.start(steps[0]);
        for (int index = 1; index < steps.length; index++) {
            flow.next(steps[index]);
        }
        return flow.build();
    }

    protected final FlowBuilder<SimpleFlow> flowBuilder(String name) {
        return new FlowBuilder<>(name + "-flow");
    }

    protected StepBuilder step(String name) {
        return factory.step(name + "-step");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        factory = new JobFactory();
        factory.afterPropertiesSet();
        super.afterPropertiesSet();
    }

    @Override
    protected void execute() throws Exception {
        factory.getSyncLauncher().run(job(), new JobParameters());
    }

    private Job job() throws Exception {
        JobBuilder jobBuilder = factory.job(ClassUtils.getShortName(getClass()));
        return jobBuilder.start(flow()).build().build();
    }

    public JobExecution executeAsync() throws Exception {
        afterPropertiesSet();
        return factory.getAsyncLauncher().run(job(), new JobParameters());
    }

    protected abstract Flow flow() throws Exception;

}
