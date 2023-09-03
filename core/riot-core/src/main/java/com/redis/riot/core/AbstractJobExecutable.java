package com.redis.riot.core;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.util.ClassUtils;

import com.redis.riot.core.StepBuilder.ReaderStepBuilder;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.AbstractRedisClient;

public abstract class AbstractJobExecutable implements Executable {

    protected final AbstractRedisClient client;

    protected JobRepository jobRepository;

    private JobBuilderFactory jobFactory;

    private StepBuilderFactory stepFactory;

    private StepOptions stepOptions = new StepOptions();

    private String name;

    private List<Consumer<StepBuilder<?, ?>>> stepConsumers = new ArrayList<>();

    protected AbstractJobExecutable(AbstractRedisClient client) {
        setName(ClassUtils.getShortName(getClass()));
        this.client = client;
    }

    public void addStepConsumer(Consumer<StepBuilder<?, ?>> consumer) {
        stepConsumers.add(consumer);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public StepOptions getStepOptions() {
        return stepOptions;
    }

    public void setStepOptions(StepOptions stepOptions) {
        this.stepOptions = stepOptions;
    }

    @Override
    public void execute() {
        try {
            jobRepository = BatchUtils.inMemoryJobRepository();
        } catch (Exception e) {
            throw new RiotExecutionException("Could not initialize job repository", e);
        }
        jobFactory = new JobBuilderFactory(jobRepository);
        stepFactory = stepBuilderFactory();
        JobExecution execution;
        try {
            execution = jobLauncher().run(job(), new JobParameters());
        } catch (JobExecutionException e) {
            // Should not happen but handle anyway
            throw new RiotExecutionException("Could not run job", e);
        }
        if (execution.getStatus().isUnsuccessful()) {
            List<Throwable> exceptions = execution.getAllFailureExceptions();
            String msg = MessageFormat.format("Error executing {0}", execution.getJobInstance().getJobName());
            if (exceptions.isEmpty()) {
                throw new RiotExecutionException(msg);
            }
            throw new RiotExecutionException(msg, exceptions.get(0));
        }
    }

    private StepBuilderFactory stepBuilderFactory() {
        return new StepBuilderFactory(jobRepository, new ResourcelessTransactionManager());
    }

    private JobLauncher jobLauncher() {
        SimpleJobLauncher launcher = new SimpleJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SyncTaskExecutor());
        return launcher;
    }

    protected JobBuilder jobBuilder() {
        return jobFactory.get(name);
    }

    protected abstract Job job();

    protected ReaderStepBuilder step(String name) {
        return StepBuilder.factory(stepFactory).name(name).options(stepOptions);
    }

    protected Step build(StepBuilder<?, ?> step) {
        stepConsumers.forEach(c -> c.accept(step));
        return step.build().build();
    }

}
