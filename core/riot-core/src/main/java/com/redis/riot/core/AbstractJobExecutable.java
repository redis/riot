package com.redis.riot.core;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

@SuppressWarnings("deprecation")
public abstract class AbstractJobExecutable extends AbstractRedisExecutable {

    private static final String DATE_VARIABLE_NAME = "date";

    private static final String REDIS_VARIABLE_NAME = "redis";

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected JobRepository jobRepository;

    private JobBuilderFactory jobFactory;

    private StepBuilderFactory stepFactory;

    private StepOptions stepOptions = new StepOptions();

    private String name;

    private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();

    private List<StepConfigurationStrategy> stepConfigurationStrategies = new ArrayList<>();

    protected AbstractJobExecutable() {
        setName(ClassUtils.getShortName(getClass()));
    }

    public void addStepConfigurationStrategy(StepConfigurationStrategy strategy) {
        stepConfigurationStrategies.add(strategy);
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

    public EvaluationContextOptions getEvaluationContextOptions() {
        return evaluationContextOptions;
    }

    public void setEvaluationContextOptions(EvaluationContextOptions evaluationContextOptions) {
        this.evaluationContextOptions = evaluationContextOptions;
    }

    @Override
    protected void execute(RiotExecutionContext executionContext) {
        checkJobRepository();
        jobFactory = new JobBuilderFactory(jobRepository);
        stepFactory = stepBuilderFactory();
        Job job = job(executionContext);
        JobExecution execution;
        try {
            execution = jobLauncher().run(job, new JobParameters());
        } catch (JobExecutionException e) {
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

    private void checkJobRepository() {
        if (jobRepository == null) {
            MapJobRepositoryFactoryBean bean = new MapJobRepositoryFactoryBean();
            try {
                bean.afterPropertiesSet();
                jobRepository = bean.getObject();
            } catch (Exception e) {
                throw new RiotExecutionException("Could not initialize job repository", e);
            }
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

    protected abstract Job job(RiotExecutionContext executionContext);

    protected <I, O> StepBuilder<I, O> createStep() {
        StepBuilder<I, O> step = new StepBuilder<>(stepFactory);
        step.name(getName());
        step.options(stepOptions);
        step.configurationStrategies(stepConfigurationStrategies);
        return step;
    }

    protected StandardEvaluationContext evaluationContext(RiotExecutionContext executionContext) {
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.setVariable(DATE_VARIABLE_NAME, new SimpleDateFormat(evaluationContextOptions.getDateFormat()));
        context.setVariable(REDIS_VARIABLE_NAME, executionContext.getRedisConnection().sync());
        if (!CollectionUtils.isEmpty(evaluationContextOptions.getVariables())) {
            evaluationContextOptions.getVariables().forEach(context::setVariable);
        }
        if (!CollectionUtils.isEmpty(evaluationContextOptions.getExpressions())) {
            evaluationContextOptions.getExpressions().forEach((k, v) -> context.setVariable(k, v.getValue(context)));
        }
        return context;
    }

}
