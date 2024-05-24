package com.redis.riot.core;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
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
import org.springframework.beans.factory.BeanInitializationException;
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

@Command
public abstract class AbstractJobCommand extends AbstractCommand {

	@ArgGroup(exclusive = false, heading = "Job options%n")
	private JobArgs jobArgs = new JobArgs();

	protected JobRepository jobRepository;
	protected PlatformTransactionManager transactionManager;
	protected JobLauncher jobLauncher;

	public void copyTo(AbstractJobCommand target) {
		super.copyTo(target);
		target.jobArgs = jobArgs;
		target.jobRepository = jobRepository;
		target.transactionManager = transactionManager;
		target.jobLauncher = jobLauncher;
	}

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
		log.info("Creating job {}", jobArgs.getName());
		return new JobBuilder(jobArgs.getName(), jobRepository);
	}

	@Override
	protected void setup() {
		super.setup();
		if (jobArgs.getName() == null) {
			Assert.notNull(commandSpec, "Command spec not set");
			jobArgs.setName(commandSpec.name());
		}
		if (jobRepository == null) {
			try {
				jobRepository = JobUtils.jobRepositoryFactoryBean().getObject();
			} catch (Exception e) {
				throw new BeanInitializationException("Could not initialize job repository", e);
			}
			Assert.notNull(jobRepository, "Could not create job repository");
		}
		if (transactionManager == null) {
			transactionManager = JobUtils.resourcelessTransactionManager();
		}
		if (jobLauncher == null) {
			jobLauncher = taskExecutorJobLauncher();
		}
	}

	private TaskExecutorJobLauncher taskExecutorJobLauncher() {
		TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(new SyncTaskExecutor());
		try {
			launcher.afterPropertiesSet();
		} catch (Exception e) {
			throw new BeanInitializationException("Could not initialize job launcher", e);
		}
		return launcher;
	}

	@Override
	protected void execute() throws Exception {
		JobExecution jobExecution = jobLauncher.run(job(), new JobParameters());
		JobUtils.checkJobExecution(jobExecution);
	}

	protected boolean shouldShowProgress() {
		return jobArgs.getProgressArgs().getStyle() != ProgressStyle.NONE;
	}

	protected abstract Job job();

	private <I, O> TaskletStep step(Step<I, O> step) {
		return faultTolerant(simpleStepBuilder(step)).build();
	}

	protected <I, O> SimpleStepBuilder<I, O> simpleStepBuilder(Step<I, O> step) {
		if (step.getName() == null) {
			step.name("step");
		}
		step.name(jobArgs.getName() + "-" + step.getName());
		if (step.getReader() instanceof ItemStreamSupport) {
			ItemStreamSupport support = (ItemStreamSupport) step.getReader();
			Assert.notNull(support.getName(), "No name specified for reader in step " + step.getName());
			support.setName(step.getName() + "-" + support.getName());
		}
		SimpleStepBuilder<I, O> builder = simpleStepBuilder(step.getName());
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
		if (step.getFlushInterval() == null) {
			return builder;
		}
		log.info("Creating flushing step with flush interval {} and idle timeout {}", step.getFlushInterval(),
				step.getIdleTimeout());
		FlushingStepBuilder<I, O> flushingStepBuilder = new FlushingStepBuilder<>(builder);
		flushingStepBuilder.flushInterval(step.getFlushInterval());
		flushingStepBuilder.idleTimeout(step.getIdleTimeout());
		return flushingStepBuilder;
	}

	private TaskExecutor taskExecutor() {
		if (jobArgs.getThreads() == 1) {
			return new SyncTaskExecutor();
		}
		log.info("Creating thread-pool task executor of size {}", jobArgs.getThreads());
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(jobArgs.getThreads());
		taskExecutor.setCorePoolSize(jobArgs.getThreads());
		taskExecutor.setQueueCapacity(jobArgs.getThreads());
		taskExecutor.initialize();
		return taskExecutor;
	}

	@SuppressWarnings("unchecked")
	private <I, O> ItemReader<? extends I> reader(Step<I, O> step) {
		if (jobArgs.getThreads() == 1 || step.getReader() instanceof AbstractPollableItemReader) {
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
		if (jobArgs.isDryRun()) {
			log.info("Using no-op writer");
			return new NoopItemWriter<>();
		}
		if (jobArgs.getSleep() > 0) {
			log.info("Throttling writer with sleep {}", jobArgs.getSleep());
			return new ThrottledItemWriter<>(step.getWriter(), jobArgs.getSleep());
		}
		return step.getWriter();
	}

	private <I, O> SimpleStepBuilder<I, O> simpleStepBuilder(String name) {
		log.info("Creating step {} with chunk size {}", name, jobArgs.getChunkSize());
		return new StepBuilder(name, jobRepository).chunk(jobArgs.getChunkSize(), transactionManager);
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		log.info("Creating fault-tolerant step with skip limit {} and retry limit {}", jobArgs.getSkipLimit(),
				jobArgs.getRetryLimit());
		FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant();
		ftStep.skipLimit(jobArgs.getSkipLimit());
		ftStep.retryLimit(jobArgs.getRetryLimit());
		return ftStep;
	}

	protected void configure(AbstractAsyncItemReader<?, ?> reader) {
		reader.setJobRepository(jobRepository);
	}

	public JobArgs getJobArgs() {
		return jobArgs;
	}

	public void setJobArgs(JobArgs args) {
		this.jobArgs = args;
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
