package com.redis.riot.cli.common;

import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobBuilderException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.riot.cli.Main;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(usageHelpAutoWidth = true)
public abstract class AbstractCommand implements Callable<Integer> {

    protected final Logger log = Logger.getLogger(getClass().getName());

    private static final int EXIT_VALUE_SUCCESS = 0;

    @ParentCommand
    private Main riot;

    @Spec
    private CommandSpec commandSpec;

    @Mixin
    private HelpOptions helpOptions;

    @ArgGroup(exclusive = false, heading = "Job options%n")
    protected JobOptions jobOptions = new JobOptions();

    private JobRepository jobRepository;

    private JobLauncher jobLauncher;

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    private PlatformTransactionManager transactionManager;

    protected JobRepository getJobRepository() {
        if (jobRepository == null) {
            try {
                jobRepository = Utils.inMemoryJobRepository();
            } catch (Exception e) {
                throw new JobBuilderException(e);
            }
        }
        return jobRepository;
    }

    private JobBuilderFactory jobBuilderFactory() {
        if (jobBuilderFactory == null) {
            jobBuilderFactory = new JobBuilderFactory(getJobRepository());
        }
        return jobBuilderFactory;
    }

    protected StepBuilderFactory stepBuilderFactory() {
        if (stepBuilderFactory == null) {
            stepBuilderFactory = new StepBuilderFactory(getJobRepository(), transactionManager());
        }
        return stepBuilderFactory;
    }

    private PlatformTransactionManager transactionManager() {
        if (transactionManager == null) {
            transactionManager = new ResourcelessTransactionManager();
        }
        return transactionManager;
    }

    private String commandName;

    public void setCommandName(String commandName) {
        this.commandName = commandName;
    }

    protected String commandName() {
        if (commandName == null) {
            return commandSpec.qualifiedName("-");
        }
        return commandName;
    }

    public void setRiot(Main riot) {
        this.riot = riot;
    }

    public void setCommandSpec(CommandSpec commandSpec) {
        this.commandSpec = commandSpec;
    }

    protected RedisOptions getRedisOptions() {
        return riot.getRedisOptions();
    }

    @Override
    public Integer call() throws Exception {
        RedisOptions redisOptions = getRedisOptions();
        RedisURI redisURI = RiotUtils.redisURI(redisOptions);
        AbstractRedisClient redisClient = RiotUtils.client(redisURI, redisOptions);
        try (CommandContext context = context(redisURI, redisClient)) {
            log.log(Level.INFO, "Creating job for {0}", this);
            Job job = job(context);
            JobExecution execution = jobLauncher().run(job, new JobParameters());
            if (execution.getStatus().isUnsuccessful()) {
                List<Throwable> exceptions = execution.getAllFailureExceptions();
                String msg = MessageFormat.format("Error executing {0}", execution.getJobInstance().getJobName());
                if (exceptions.isEmpty()) {
                    throw new JobExecutionException(msg);
                }
                Throwable throwable = exceptions.get(0);
                if (throwable instanceof Exception) {
                    throw (Exception) throwable;
                }
                if (throwable instanceof RuntimeException) {
                    throw (RuntimeException) throwable;
                }
                throw new JobExecutionException(msg, throwable);
            }
            return EXIT_VALUE_SUCCESS;
        }

    }

    private JobLauncher jobLauncher() {
        if (jobLauncher == null) {
            SimpleJobLauncher launcher = new SimpleJobLauncher();
            launcher.setJobRepository(getJobRepository());
            launcher.setTaskExecutor(new SyncTaskExecutor());
            jobLauncher = launcher;
        }
        return jobLauncher;
    }

    protected CommandContext context(RedisURI redisURI, AbstractRedisClient redisClient) {
        return new CommandContext(redisURI, redisClient);
    }

    protected abstract Job job(CommandContext context) throws Exception;

    protected <I, O> Job job(RiotStep<I, O> step) {
        if (step.getName() == null) {
            step.name(commandName());
        }
        log.log(Level.INFO, "Creating job using {0}", this);
        return job(step.getName()).start(step.build().build()).build();
    }

    protected JobBuilder job(String name) {
        return jobBuilderFactory().get(name);
    }

    public JobOptions getJobOptions() {
        return jobOptions;
    }

    public void setJobOptions(JobOptions options) {
        this.jobOptions = options;
    }

    protected <I, O> RiotStep<I, O> step(ItemReader<I> reader, ItemWriter<O> writer) {
        return new RiotStep<>(stepBuilderFactory(), reader, writer).options(jobOptions);
    }

}
