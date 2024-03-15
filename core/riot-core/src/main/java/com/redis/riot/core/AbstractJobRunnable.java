package com.redis.riot.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.hsqldb.jdbc.JDBCDataSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties.Jdbc;
import org.springframework.boot.sql.init.AbstractScriptDatabaseInitializer;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.writer.AbstractOperationItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;

public abstract class AbstractJobRunnable extends AbstractRunnable {

	public static final SkipPolicy DEFAULT_SKIP_POLICY = new NeverSkipItemSkipPolicy();
	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;
	public static final Duration DEFAULT_SLEEP = Duration.ZERO;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_THREADS = 1;

	private static final String FAILED_JOB_MESSAGE = "Error executing job %s";

	private String name;
	private Consumer<RiotStep<?, ?>> stepConfigurer;
	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private Duration sleep = DEFAULT_SLEEP;
	private boolean dryRun;
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private int retryLimit = DEFAULT_RETRY_LIMIT;
	private JobRepository jobRepository;
	private PlatformTransactionManager transactionManager;

	protected AbstractJobRunnable() {
		setName(ClassUtils.getShortName(getClass()));
	}

	protected String name(String... suffixes) {
		List<String> elements = new ArrayList<>();
		elements.add(name);
		elements.addAll(Arrays.asList(suffixes));
		return String.join("-", elements);
	}

	public void setStepConfigurer(Consumer<RiotStep<?, ?>> stepConfigurer) {
		this.stepConfigurer = stepConfigurer;
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public void setTransactionManager(PlatformTransactionManager platformTransactionManager) {
		this.transactionManager = platformTransactionManager;
	}

	@Override
	protected void open() {
		super.open();
		if (transactionManager == null) {
			transactionManager = new ResourcelessTransactionManager();
		}
		if (jobRepository == null) {
			JobRepositoryFactoryBean bean = new JobRepositoryFactoryBean();
			bean.setDataSource(dataSource());
			bean.setDatabaseType("HSQL");
			bean.setTransactionManager(transactionManager);
			try {
				bean.afterPropertiesSet();
				jobRepository = bean.getObject();
			} catch (Exception e) {
				throw new ExecutionException("Could not initialize job repository", e);
			}
		}
	}

	private DataSource dataSource() {
		JDBCDataSource source = new JDBCDataSource();
		source.setURL("jdbc:hsqldb:mem:" + name);
		Jdbc jdbc = new Jdbc();
		jdbc.setInitializeSchema(DatabaseInitializationMode.ALWAYS);
		AbstractScriptDatabaseInitializer initializer = new BatchDataSourceScriptDatabaseInitializer(source, jdbc);
		initializer.initializeDatabase();
		return source;
	}

	@Override
	protected void doRun() {
		Job job = job();
		TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		JobExecution jobExecution;
		try {
			jobExecution = jobLauncher.run(job, new JobParameters());
		} catch (JobExecutionException e) {
			throw new ExecutionException(String.format(FAILED_JOB_MESSAGE, job.getName()), e);
		}
		if (jobExecution.getExitStatus().getExitCode().equals(ExitStatus.FAILED.getExitCode())) {
			for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
				ExitStatus exitStatus = stepExecution.getExitStatus();
				if (exitStatus.getExitCode().equals(ExitStatus.FAILED.getExitCode())) {
					String message = String.format("Error executing step %s in job %s: %s", stepExecution.getStepName(),
							job.getName(), exitStatus.getExitDescription());
					if (stepExecution.getFailureExceptions().isEmpty()) {
						throw new ExecutionException(message);
					}
					throw new ExecutionException(message, stepExecution.getFailureExceptions().get(0));
				}
			}
			if (jobExecution.getAllFailureExceptions().isEmpty()) {
				throw new ExecutionException(String.format("Error executing job %s: %s", job.getName(),
						jobExecution.getExitStatus().getExitDescription()));
			}
		}
	}

	protected JobBuilder jobBuilder() {
		return new JobBuilder(getName(), jobRepository);
	}

	protected abstract Job job();

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

	protected <I, O> FaultTolerantStepBuilder<I, O> step(String name, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		RiotStep<I, O> riotStep = new RiotStep<>();
		riotStep.setName(name);
		riotStep.setReader(reader);
		riotStep.setProcessor(processor);
		riotStep.setWriter(writer);
		if (stepConfigurer != null) {
			stepConfigurer.accept(riotStep);
		}
		SimpleStepBuilder<I, O> step = new StepBuilder(riotStep.getName(), jobRepository).chunk(chunkSize,
				transactionManager);
		step.reader(reader(riotStep.getReader()));
		step.processor(riotStep.getProcessor());
		step.writer(writer(riotStep.getWriter()));
		step.taskExecutor(taskExecutor());
		riotStep.getConfigurer().accept(step);
		if (riotStep.getReader() instanceof RedisItemReader) {
			RedisItemReader<?, ?, ?> redisReader = (RedisItemReader<?, ?, ?>) riotStep.getReader();
			if (redisReader.isLive()) {
				step = new FlushingStepBuilder<>(step).interval(redisReader.getFlushInterval())
						.idleTimeout(redisReader.getIdleTimeout());
			}
		}
		return faultTolerant(step);
	}

	private <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant();
		ftStep.skipLimit(skipLimit);
		ftStep.retryLimit(retryLimit);
		ftStep.retry(RedisCommandTimeoutException.class);
		ftStep.noRetry(RedisCommandExecutionException.class);
		return ftStep;
	}

	private TaskExecutor taskExecutor() {
		if (isMultiThreaded()) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.setQueueCapacity(threads);
			taskExecutor.initialize();
			return taskExecutor;
		}
		return new SyncTaskExecutor();
	}

	private <T> ItemReader<T> reader(ItemReader<T> reader) {
		if (reader instanceof RedisItemReader) {
			return reader;
		}
		if (isMultiThreaded()) {
			if (reader instanceof ItemStreamReader) {
				SynchronizedItemStreamReader<T> synchronizedReader = new SynchronizedItemStreamReader<>();
				synchronizedReader.setDelegate((ItemStreamReader<T>) reader);
				return synchronizedReader;
			}
			return new SynchronizedItemReader<>(reader);
		}
		return reader;
	}

	private boolean isMultiThreaded() {
		return threads > 1;
	}

	private <T> ItemWriter<T> writer(ItemWriter<T> writer) {
		if (dryRun) {
			return new NoopItemWriter<>();
		}
		if (RiotUtils.isPositive(sleep)) {
			return new ThrottledItemWriter<>(writer, sleep);
		}
		return writer;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public Duration getSleep() {
		return sleep;
	}

	public void setSleep(Duration sleep) {
		this.sleep = sleep;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	public int getRetryLimit() {
		return retryLimit;
	}

	public void setRetryLimit(int retryLimit) {
		this.retryLimit = retryLimit;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	protected JobRepository getJobRepository() {
		return jobRepository;
	}

	protected PlatformTransactionManager getTransactionManager() {
		return transactionManager;
	}

}
