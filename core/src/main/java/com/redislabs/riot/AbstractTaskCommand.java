package com.redislabs.riot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
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
    protected void execute() throws Exception {
        jobFactory.getSyncLauncher().run(job(), new JobParameters());
    }

    private Job job() throws Exception {
        return jobFactory.job(ClassUtils.getShortName(getClass())).start(flow()).build().build();
    }

    /**
     * For unit-testing
     *
     * @return
     * @throws Exception
     */
    public JobExecution executeAsync() throws Exception {
        afterPropertiesSet();
        JobExecution execution = jobFactory.getAsyncLauncher().run(job(), new JobParameters());
        while (!execution.isRunning()) {
            Thread.sleep(10);
        }
        return execution;
    }

    protected abstract Flow flow() throws Exception;

}
