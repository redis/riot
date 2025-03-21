package com.redis.riot.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.item.AbstractAsyncItemStreamSupport;
import com.redis.spring.batch.item.PollableItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractJobCommand extends AbstractCallableCommand {

	public static final String DEFAULT_JOB_REPOSITORY_NAME = "riot";

	@Option(names = "--job-name", description = "Job name.", paramLabel = "<string>", hidden = true)
	private String jobName;

	@Option(names = "--repeat", description = "After the job completes keep repeating it on a fixed interval (ex 5m, 1h)", paramLabel = "<dur>")
	private RiotDuration repeatEvery;

	@ArgGroup(exclusive = false, heading = "Job options%n")
	private StepArgs stepArgs = new StepArgs();

	private String jobRepositoryName = DEFAULT_JOB_REPOSITORY_NAME;
	private JobRepository jobRepository;
	private PlatformTransactionManager transactionManager;
	private JobLauncher jobLauncher;
	private JobExplorer jobExplorer;

	protected Runnable onJobSuccessCallback;

	@Override
	protected void initialize() {
		super.initialize();
		if (jobName == null) {
			jobName = jobName();
		}
		if (jobRepository == null) {
			try {
				jobRepository = JobUtils.jobRepositoryFactoryBean(jobRepositoryName).getObject();
			} catch (Exception e) {
				throw new RiotException("Could not create job repository", e);
			}
		}
		if (transactionManager == null) {
			transactionManager = JobUtils.resourcelessTransactionManager();
		}
		if (jobLauncher == null) {
			try {
				jobLauncher = jobLauncher();
			} catch (Exception e) {
				throw new RiotException("Could not create job launcher", e);
			}
		}
		if (jobExplorer == null) {
			try {
				jobExplorer = JobUtils.jobExplorerFactoryBean(jobRepositoryName).getObject();
			} catch (Exception e) {
				log.warn("Error getting jobExplorer", e);
				throw new RiotException("Could not create job explorer", e);
			}
		}
	}

	private JobLauncher jobLauncher() throws Exception {
		TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(new SyncTaskExecutor());
		launcher.afterPropertiesSet();
		return launcher;
	}

	protected void configureAsyncStreamSupport(AbstractAsyncItemStreamSupport<?, ?> reader) {
		reader.setJobRepository(jobRepository);
	}

	private JobBuilder jobBuilder() {
		return new JobBuilder(jobName, jobRepository);
	}

	@Override
	protected void execute() {
		Job job = job();
		JobExecution jobExecution;
		try {
			jobExecution = jobLauncher.run(job, new JobParameters());
		} catch (JobExecutionException e) {
			throw new RiotException(e);
		}
		if (JobUtils.isFailed(jobExecution.getExitStatus())) {
			for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
				ExitStatus stepExitStatus = stepExecution.getExitStatus();
				if (JobUtils.isFailed(stepExitStatus)) {
					if (CollectionUtils.isEmpty(stepExecution.getFailureExceptions())) {
						throw new RiotException(stepExitStatus.getExitDescription());
					}
					throw wrapException(stepExecution.getFailureExceptions());
				}
			}
			throw wrapException(jobExecution.getFailureExceptions());
		}
	}

	private String jobName() {
		if (commandSpec == null) {
			return ClassUtils.getShortName(getClass());
		}
		return commandSpec.name();
	}

	private RiotException wrapException(List<Throwable> throwables) {
		if (throwables.isEmpty()) {
			return new RiotException("Job failed");
		}
		return new RiotException(throwables.get(0));
	}

	protected Job job(Step<?, ?>... steps) {
		return job(Arrays.asList(steps));
	}

	protected Job job(Collection<Step<?, ?>> steps) {
		Assert.notEmpty(steps, "At least one step must be specified");
		Iterator<Step<?, ?>> iterator = steps.iterator();
		SimpleJobBuilder job = jobBuilder().start(step(iterator.next()));
		while (iterator.hasNext()) {
			job.next(step(iterator.next()));
		}

		if (repeatEvery != null) {
			job.incrementer(new RunIdIncrementer());
			job.preventRestart();
			job.listener(new RepeatJobExecutionListener(job, steps));
		}

		return job.build();
	}

	private class RepeatJobExecutionListener implements JobExecutionListener {

		private final SimpleJobBuilder job;
		private final Collection<Step<?, ?>> steps;
		private Job lastJob;

		public RepeatJobExecutionListener(SimpleJobBuilder job, Collection<Step<?, ?>> steps) {
			this.job = job;
			this.steps = steps;
		}

		@Override
		public void afterJob(JobExecution jobExecution) {
			if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
				if (null != onJobSuccessCallback) {
					onJobSuccessCallback.run();
				}

				log.info("Finished job, will run again in {}", repeatEvery);
				try {
					Thread.sleep(repeatEvery.getValue().toMillis());
					if (lastJob == null) {
						lastJob = job.build();
					}

					Job nextJob = jobBuilder().start(step(steps.stream().findFirst().get()))
							.incrementer(new RunIdIncrementer()).preventRestart().listener(this).build();

					JobParametersBuilder paramsBuilder = new JobParametersBuilder(jobExecution.getJobParameters(),
							jobExplorer);

					jobLauncher.run(nextJob,
							paramsBuilder.addString("runTime", String.valueOf(System.currentTimeMillis()))
									.getNextJobParameters(lastJob).toJobParameters());
					lastJob = nextJob;
				} catch (InterruptedException | JobExecutionAlreadyRunningException | JobRestartException
						| JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
					throw new RiotException(e);
				}
			}
			JobExecutionListener.super.afterJob(jobExecution);
		}
	}

	protected boolean shouldShowProgress() {
		return stepArgs.getProgressArgs().getStyle() != ProgressStyle.NONE;
	}

	protected abstract Job job();

	private <I, O> TaskletStep step(Step<I, O> step) {
		log.info("Creating {}", step);
		SimpleStepBuilder<I, O> builder = simpleStep(step);
		if (stepArgs.getRetryPolicy() == RetryPolicy.NEVER && stepArgs.getSkipPolicy() == SkipPolicy.NEVER) {
			log.info("Skipping fault-tolerance for step {}", step.getName());
			return builder.build();
		}
		log.info("Adding fault-tolerance to step {}", step.getName());
		FaultTolerantStepBuilder<I, O> ftStep = builder.faultTolerant();
		step.getSkip().forEach(ftStep::skip);
		step.getNoSkip().forEach(ftStep::noSkip);
		step.getRetry().forEach(ftStep::retry);
		step.getNoRetry().forEach(ftStep::noRetry);
		ftStep.retryLimit(stepArgs.getRetryLimit());
		ftStep.retryPolicy(retryPolicy());
		ftStep.skipLimit(stepArgs.getSkipLimit());
		ftStep.skipPolicy(skipPolicy());
		return ftStep.build();
	}

	private org.springframework.retry.RetryPolicy retryPolicy() {
		switch (stepArgs.getRetryPolicy()) {
		case ALWAYS:
			return new AlwaysRetryPolicy();
		case NEVER:
			return new NeverRetryPolicy();
		default:
			return null;
		}
	}

	private org.springframework.batch.core.step.skip.SkipPolicy skipPolicy() {
		switch (stepArgs.getSkipPolicy()) {
		case ALWAYS:
			return new AlwaysSkipItemSkipPolicy();
		case NEVER:
			return new NeverSkipItemSkipPolicy();
		default:
			return null;
		}
	}

	@SuppressWarnings("removal")
	private <I, O> SimpleStepBuilder<I, O> simpleStep(Step<I, O> step) {
		String stepName = jobName + "-" + step.getName();
		if (stepName.length() > 80) {
			stepName = stepName.substring(0, 69) + "…" + stepName.substring(stepName.length() - 10);
		}
		if (step.getReader() instanceof ItemStreamSupport) {
			((ItemStreamSupport) step.getReader()).setName(stepName + "-reader");
		}
		log.info("Creating step {} with chunk size {}", stepName, stepArgs.getChunkSize());
		SimpleStepBuilder<I, O> builder = new StepBuilder(stepName, jobRepository).<I, O>chunk(stepArgs.getChunkSize(),
				transactionManager);
		builder.reader(reader(step));
		builder.writer(writer(step));
		builder.processor(step.getProcessor());
		builder.taskExecutor(taskExecutor());
		builder.throttleLimit(stepArgs.getThreads());
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
		taskExecutor.initialize();
		return taskExecutor;
	}

	private <I, O> ItemReader<? extends I> reader(Step<I, O> step) {
		if (stepArgs.getThreads() == 1 || step.getReader() instanceof PollableItemReader) {
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
		ItemWriter<O> writer = step.getWriter();
		if (stepArgs.isDryRun()) {
			log.info("Using no-op writer");
			writer = new NoopItemWriter<>();
		}
		if (stepArgs.getSleep() != null) {
			log.info("Throttling writer with sleep={}", stepArgs.getSleep());
			writer = new ThrottledItemWriter<>(writer, stepArgs.getSleep().getValue());
		}
		return writer;
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

	public JobExplorer getJobExplorer() {
		return jobExplorer;
	}

	public void setJobExplorer(JobExplorer jobExplorer) {
		this.jobExplorer = jobExplorer;
	}
}
