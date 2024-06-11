package com.redis.riot.core;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.item.AbstractAsyncItemReader;
import com.redis.spring.batch.item.AbstractPollableItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractJobCommand extends AbstractCommand {

	public static final String DEFAULT_JOB_REPOSITORY_NAME = "riot";

	@Option(names = "--job-name", description = "Job name.", paramLabel = "<string>", hidden = true)
	protected String jobName;

	@ArgGroup(exclusive = false, heading = "Job options%n")
	private StepArgs stepArgs = new StepArgs();

	private String jobRepositoryName = DEFAULT_JOB_REPOSITORY_NAME;
	protected JobRepository jobRepository;
	protected PlatformTransactionManager transactionManager;
	protected JobLauncher jobLauncher;

	protected Job job(Step<?, ?>... steps) {
		return job(Stream.of(steps));
	}

	protected Job job(Iterable<Step<?, ?>> steps) {
		return job(StreamSupport.stream(steps.spliterator(), false));
	}

	private Job job(Stream<Step<?, ?>> steps) {
		Iterator<TaskletStep> iterator = steps.map(this::step).iterator();
		SimpleJobBuilder job = jobBuilder().start(iterator.next());
		while (iterator.hasNext()) {
			job.next(iterator.next());
		}
		return job.build();
	}

	private JobBuilder jobBuilder() {
		log.info("Creating job {}", jobName);
		return new JobBuilder(jobName, jobRepository);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		if (jobName == null) {
			Assert.notNull(commandSpec, "Command spec not set");
			jobName = commandSpec.name();
		}
		if (jobRepository == null) {
			jobRepository = JobUtils.jobRepositoryFactoryBean(jobRepositoryName).getObject();
			Assert.notNull(jobRepository, "Could not create job repository");
		}
		if (transactionManager == null) {
			transactionManager = JobUtils.resourcelessTransactionManager();
		}
		if (jobLauncher == null) {
			jobLauncher = taskExecutorJobLauncher();
		}
	}

	private TaskExecutorJobLauncher taskExecutorJobLauncher() throws Exception {
		TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(new SyncTaskExecutor());
		launcher.afterPropertiesSet();
		return launcher;
	}

	@Override
	protected void execute() {
		Job job = job();
		JobExecution jobExecution;
		try {
			jobExecution = jobLauncher.run(job, new JobParameters());
		} catch (JobExecutionException e) {
			throw new RiotException("Could not run job " + job.getName(), e);
		} finally {
			shutdown();
		}
		if (JobUtils.isFailed(jobExecution.getExitStatus())) {
			for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
				if (JobUtils.isFailed(stepExecution.getExitStatus())) {
					throw new RiotException(stepExecution.getExitStatus().getExitDescription());
				}
			}
			throw new RiotException(jobExecution.getExitStatus().getExitDescription());
		}
	}

	protected abstract void shutdown();

	protected boolean shouldShowProgress() {
		return stepArgs.getProgressArgs().getStyle() != ProgressStyle.NONE;
	}

	protected abstract Job job();

	private <I, O> TaskletStep step(Step<I, O> step) {
		SimpleStepBuilder<I, O> builder = simpleStepBuilder(step);
		if (stepArgs.getRetryPolicy() == RetryPolicy.NEVER && stepArgs.getSkipPolicy() == SkipPolicy.NEVER) {
			return builder.build();
		}
		FaultTolerantStepBuilder<I, O> ftStep = JobUtils.faultTolerant(builder);
		if (stepArgs.getSkipPolicy() == SkipPolicy.LIMIT) {
			ftStep.skipLimit(stepArgs.getSkipLimit());
			step.getSkip().forEach(ftStep::skip);
			step.getNoSkip().forEach(ftStep::noSkip);
		} else {
			ftStep.skipPolicy(stepArgs.skipPolicy());
		}
		if (stepArgs.getRetryPolicy() == RetryPolicy.LIMIT) {
			ftStep.retryLimit(stepArgs.getRetryLimit());
			step.getRetry().forEach(ftStep::retry);
			step.getNoRetry().forEach(ftStep::noRetry);
		} else {
			ftStep.retryPolicy(stepArgs.retryPolicy());
		}
		return ftStep.build();
	}

	private <I, O> SimpleStepBuilder<I, O> simpleStepBuilder(Step<I, O> step) {
		String stepName = jobName + "-" + step.getName();
		if (step.getReader() instanceof ItemStreamSupport) {
			ItemStreamSupport support = (ItemStreamSupport) step.getReader();
			Assert.notNull(support.getName(), "No name specified for reader in step " + stepName);
			support.setName(stepName + "-" + support.getName());
		}
		log.info("Creating step {} with chunk size {}", stepName, stepArgs.getChunkSize());
		SimpleStepBuilder<I, O> builder = new StepBuilder(stepName, jobRepository).chunk(stepArgs.getChunkSize(),
				transactionManager);
		builder.reader(reader(step));
		builder.writer(writer(step));
		builder.processor(step.getProcessor());
		builder.taskExecutor(taskExecutor());
		step.getExecutionListeners().forEach(builder::listener);
		step.getWriteListeners().forEach(builder::listener);
		if (shouldShowProgress()) {
			ProgressStepExecutionListener<I, O> listener = new ProgressStepExecutionListener<>(step);
			builder.listener((StepExecutionListener) listener);
			builder.listener((ItemWriteListener<?>) listener);
		}
		if (step.isLive()) {
			log.info("Creating flushing step with flush interval {} and idle timeout {}", step.getFlushInterval(),
					step.getIdleTimeout());
			FlushingStepBuilder<I, O> flushingStepBuilder = new FlushingStepBuilder<>(builder);
			flushingStepBuilder.flushInterval(step.getFlushInterval());
			flushingStepBuilder.idleTimeout(step.getIdleTimeout());
			return flushingStepBuilder;
		}
		return builder;
	}

	private TaskExecutor taskExecutor() {
		if (stepArgs.getThreads() == 1) {
			return new SyncTaskExecutor();
		}
		log.info("Creating thread-pool task executor of size {}", stepArgs.getThreads());
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(stepArgs.getThreads());
		taskExecutor.setCorePoolSize(stepArgs.getThreads());
		taskExecutor.setQueueCapacity(stepArgs.getThreads());
		taskExecutor.initialize();
		return taskExecutor;
	}

	private <I, O> ItemReader<? extends I> reader(Step<I, O> step) {
		if (stepArgs.getThreads() == 1 || step.getReader() instanceof AbstractPollableItemReader) {
			return step.getReader();
		}
		log.info("Synchronizing reader in step {}", step.getName());
		if (step.getReader() instanceof ItemStreamReader) {
			SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate((ItemStreamReader<I>) step.getReader());
			return synchronizedReader;
		}
		return new SynchronizedItemReader<>(step.getReader());
	}

	private <I, O> ItemWriter<? super O> writer(Step<I, O> step) {
		if (stepArgs.isDryRun()) {
			log.info("Using no-op writer");
			return new NoopItemWriter<>();
		}
		if (stepArgs.getSleep() > 0) {
			log.info("Throttling writer with sleep {}", stepArgs.getSleep());
			return new ThrottledItemWriter<>(step.getWriter(), stepArgs.getSleep());
		}
		return step.getWriter();
	}

	protected void configure(AbstractAsyncItemReader<?, ?> reader) {
		reader.setJobRepository(jobRepository);
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String name) {
		this.jobName = name;
	}

	public StepArgs getJobArgs() {
		return stepArgs;
	}

	public void setJobArgs(StepArgs args) {
		this.stepArgs = args;
	}

	public String getJobRepositoryName() {
		return jobRepositoryName;
	}

	public void setJobRepositoryName(String jobRepositoryName) {
		this.jobRepositoryName = jobRepositoryName;
	}

	public JobRepository getJobRepository() {
		return jobRepository;
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public PlatformTransactionManager getTransactionManager() {
		return transactionManager;
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	public JobLauncher getJobLauncher() {
		return jobLauncher;
	}

	public void setJobLauncher(JobLauncher jobLauncher) {
		this.jobLauncher = jobLauncher;
	}

}
