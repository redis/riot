package com.redis.riot.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.AbstractJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.writer.AbstractOperationItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;

public abstract class AbstractJobRunnable extends AbstractRiotRunnable {

    private static final String FAILED_JOB_MESSAGE = "Error executing job %s";

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String name = ClassUtils.getShortName(getClass());

    protected JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    private StepOptions stepOptions = new StepOptions();

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    private SimpleJobLauncher jobLauncher;

    private Consumer<RiotStep<?, ?>> stepConfigurer = s -> {
    };

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    protected String name(String... suffixes) {
        List<String> elements = new ArrayList<>();
        elements.add(name);
        elements.addAll(Arrays.asList(suffixes));
        return String.join("-", elements);
    }

    public StepOptions getStepOptions() {
        return stepOptions;
    }

    public void setStepOptions(StepOptions stepOptions) {
        this.stepOptions = stepOptions;
    }

    public void setJobRepository(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setStepConfigurer(Consumer<RiotStep<?, ?>> stepConfigurer) {
        this.stepConfigurer = stepConfigurer;
    }

    @Override
    protected void execute(RiotContext executionContext) {
        initialize();
        Job job = job(executionContext);
        JobExecution jobExecution;
        try {
            jobExecution = jobLauncher.run(job, new JobParameters());
        } catch (JobExecutionException e) {
            throw new RiotExecutionException(String.format(FAILED_JOB_MESSAGE, job.getName()), e);
        }
        if (jobExecution.getExitStatus().getExitCode().equals(ExitStatus.FAILED.getExitCode())) {
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                ExitStatus exitStatus = stepExecution.getExitStatus();
                if (exitStatus.getExitCode().equals(ExitStatus.FAILED.getExitCode())) {
                    String message = String.format("Error executing step %s in job %s: %s", stepExecution.getStepName(),
                            job.getName(), exitStatus.getExitDescription());
                    if (stepExecution.getFailureExceptions().isEmpty()) {
                        throw new RiotExecutionException(message);
                    }
                    throw new RiotExecutionException(message, stepExecution.getFailureExceptions().get(0));
                }
            }
            if (jobExecution.getAllFailureExceptions().isEmpty()) {
                throw new RiotExecutionException(String.format("Error executing job %s: %s", job.getName(),
                        jobExecution.getExitStatus().getExitDescription()));
            }
        }
    }

    protected JobBuilder jobBuilder() {
        return jobBuilderFactory.get(name);
    }

    protected void initialize() {
        if (jobRepository == null || transactionManager == null) {
            @SuppressWarnings("deprecation")
            AbstractJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
            if (jobRepository == null) {
                try {
                    jobRepository = bean.getObject();
                } catch (Exception e) {
                    throw new RiotExecutionException("Could not initialize job repository", e);
                }
            }
            if (transactionManager == null) {
                transactionManager = bean.getTransactionManager();
            }
        }
        jobBuilderFactory = new JobBuilderFactory(jobRepository);
        stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
        jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SyncTaskExecutor());
    }

    protected abstract Job job(RiotContext executionContext);

    protected <W extends AbstractOperationItemWriter<?, ?, ?>> W writer(W writer, RedisWriterOptions options) {
        writer.setMultiExec(options.isMultiExec());
        writer.setPoolSize(options.getPoolSize());
        writer.setWaitReplicas(options.getWaitReplicas());
        writer.setWaitTimeout(options.getWaitTimeout());
        if (writer instanceof StructItemWriter) {
            ((StructItemWriter<?, ?>) writer).setMerge(options.isMerge());
        }
        return writer;
    }

    protected <I, O> FaultTolerantStepBuilder<I, O> step(String name, ItemReader<I> reader, ItemWriter<O> writer) {
        return step(name, reader, null, writer);
    }

    protected <I, O> FaultTolerantStepBuilder<I, O> step(String name, ItemReader<I> reader, ItemProcessor<I, O> processor,
            ItemWriter<O> writer) {
        RiotStep<I, O> step = new RiotStep<>();
        step.setName(name);
        step.setReader(reader);
        step.setProcessor(processor);
        step.setWriter(writer);
        stepConfigurer.accept(step);
        return step(step);
    }

    protected <I, O> FaultTolerantStepBuilder<I, O> step(RiotStep<I, O> riotStep) {
        SimpleStepBuilder<I, O> step = stepBuilderFactory.get(riotStep.getName()).chunk(stepOptions.getChunkSize());
        step.reader(reader(riotStep.getReader()));
        step.processor(processor(riotStep.getProcessor()));
        step.writer(writer(riotStep.getWriter()));
        if (stepOptions.getThreads() > 1) {
            ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
            taskExecutor.setMaxPoolSize(stepOptions.getThreads());
            taskExecutor.setCorePoolSize(stepOptions.getThreads());
            taskExecutor.setQueueCapacity(stepOptions.getThreads());
            taskExecutor.afterPropertiesSet();
            step.taskExecutor(taskExecutor);
            step.throttleLimit(stepOptions.getThreads());
        }
        riotStep.getConfigurer().accept(step);
        if (riotStep.getReader() instanceof RedisItemReader) {
            RedisItemReader<?, ?, ?> redisReader = (RedisItemReader<?, ?, ?>) riotStep.getReader();
            if (redisReader.isLive()) {
                FlushingStepBuilder<I, O> flushingStep = new FlushingStepBuilder<>(step);
                flushingStep.interval(redisReader.getFlushInterval());
                flushingStep.idleTimeout(redisReader.getIdleTimeout());
                step = flushingStep;
            }
        }
        FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant();
        ftStep.skipLimit(stepOptions.getSkipLimit());
        ftStep.retryLimit(stepOptions.getRetryLimit());
        ftStep.retry(RedisCommandTimeoutException.class);
        ftStep.noRetry(RedisCommandExecutionException.class);
        return ftStep;
    }

    private <I, O> ItemProcessor<I, O> processor(ItemProcessor<I, O> processor) {
        initializeBean(processor);
        return processor;
    }

    private void initializeBean(Object object) {
        if (object instanceof InitializingBean) {
            try {
                ((InitializingBean) object).afterPropertiesSet();
            } catch (Exception e) {
                throw new RiotExecutionException("Could not initialize " + object, e);
            }
        }
    }

    private <T> ItemReader<T> reader(ItemReader<T> reader) {
        initializeBean(reader);
        if (reader instanceof RedisItemReader) {
            return reader;
        }
        if (stepOptions.getThreads() > 1 && reader instanceof ItemStreamReader) {
            SynchronizedItemStreamReader<T> synchronizedReader = new SynchronizedItemStreamReader<>();
            synchronizedReader.setDelegate((ItemStreamReader<T>) reader);
            return synchronizedReader;
        }
        return reader;
    }

    private <T> ItemWriter<T> writer(ItemWriter<T> writer) {
        if (stepOptions.isDryRun()) {
            return new NoopItemWriter<>();
        }
        initializeBean(writer);
        if (stepOptions.getSleep() == null || stepOptions.getSleep().isNegative() || stepOptions.getSleep().isZero()) {
            return writer;
        }
        return new ThrottledItemWriter<>(writer, stepOptions.getSleep());
    }

}
