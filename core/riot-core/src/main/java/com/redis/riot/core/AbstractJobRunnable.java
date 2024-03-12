package com.redis.riot.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.hsqldb.jdbc.JDBCDataSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
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
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
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

public abstract class AbstractJobRunnable extends AbstractRiotRunnable {

	public static final SkipPolicy DEFAULT_SKIP_POLICY = new NeverSkipItemSkipPolicy();
	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;
	public static final Duration DEFAULT_SLEEP = Duration.ZERO;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_THREADS = 1;

	private static final String FAILED_JOB_MESSAGE = "Error executing job %s";

	private String name = String.format("%s-%s", ClassUtils.getShortName(getClass()), UUID.randomUUID().toString());
	private Consumer<RiotStep<?, ?>> stepConfigurer;
	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private Duration sleep = DEFAULT_SLEEP;
	private boolean dryRun;
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private int retryLimit = DEFAULT_RETRY_LIMIT;

	private JobRepository jobRepository;
	private PlatformTransactionManager transactionManager;
	private TaskExecutorJobLauncher jobLauncher;

	protected String name(String... suffixes) {
		List<String> elements = new ArrayList<>();
		elements.add(name);
		elements.addAll(Arrays.asList(suffixes));
		return String.join("-", elements);
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
		Job job;
		try {
			job = job(executionContext);
		} catch (Exception e) {
			throw new RiotException("Could not create job", e);
		}
		JobExecution jobExecution;
		try {
			jobExecution = jobLauncher().run(job, new JobParameters());
		} catch (Exception e) {
			throw new RiotException(String.format(FAILED_JOB_MESSAGE, job.getName()), e);
		}
		if (jobExecution.getExitStatus().getExitCode().equals(ExitStatus.FAILED.getExitCode())) {
			for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
				ExitStatus exitStatus = stepExecution.getExitStatus();
				if (exitStatus.getExitCode().equals(ExitStatus.FAILED.getExitCode())) {
					String message = String.format("Error executing step %s in job %s: %s", stepExecution.getStepName(),
							job.getName(), exitStatus.getExitDescription());
					if (stepExecution.getFailureExceptions().isEmpty()) {
						throw new RiotException(message);
					}
					throw new RiotException(message, stepExecution.getFailureExceptions().get(0));
				}
			}
			if (jobExecution.getAllFailureExceptions().isEmpty()) {
				throw new RiotException(String.format("Error executing job %s: %s", job.getName(),
						jobExecution.getExitStatus().getExitDescription()));
			}
		}
	}

	protected JobBuilder jobBuilder() throws Exception {
		return new JobBuilder(name, jobRepository());
	}

	private TaskExecutorJobLauncher jobLauncher() throws Exception {
		if (jobLauncher == null) {
			jobLauncher = new TaskExecutorJobLauncher();
			jobLauncher.setJobRepository(jobRepository());
			jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		}
		return jobLauncher;
	}

	protected PlatformTransactionManager transactionManager() {
		if (transactionManager == null) {
			transactionManager = new ResourcelessTransactionManager();
		}
		return transactionManager;
	}

	protected JobRepository jobRepository() throws Exception {
		if (jobRepository == null) {
			JobRepositoryFactoryBean bean = new JobRepositoryFactoryBean();
			bean.setDataSource(dataSource());
			bean.setDatabaseType("HSQL");
			bean.setTransactionManager(transactionManager());
			bean.afterPropertiesSet();
			jobRepository = bean.getObject();
		}
		return jobRepository;
	}

	private DataSource dataSource() throws Exception {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setURL("jdbc:hsqldb:mem:" + name);
		BatchProperties.Jdbc jdbc = new BatchProperties.Jdbc();
		jdbc.setInitializeSchema(DatabaseInitializationMode.ALWAYS);
		BatchDataSourceScriptDatabaseInitializer initializer = new BatchDataSourceScriptDatabaseInitializer(dataSource,
				jdbc);
		initializer.afterPropertiesSet();
		initializer.initializeDatabase();
		return dataSource;
	}

	protected abstract Job job(RiotContext executionContext) throws Exception;

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
			ItemProcessor<I, O> processor, ItemWriter<O> writer) throws Exception {
		RiotStep<I, O> riotStep = new RiotStep<>();
		riotStep.setName(name);
		riotStep.setReader(reader);
		riotStep.setProcessor(processor);
		riotStep.setWriter(writer);
		if (stepConfigurer != null) {
			stepConfigurer.accept(riotStep);
		}
		SimpleStepBuilder<I, O> step = new StepBuilder(riotStep.getName(), jobRepository()).chunk(chunkSize,
				transactionManager);
		step.reader(reader(riotStep.getReader()));
		step.processor(processor(riotStep.getProcessor()));
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
		if (threads > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.setQueueCapacity(threads);
			taskExecutor.afterPropertiesSet();
			return taskExecutor;
		}
		return new SyncTaskExecutor();
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
				throw new RiotException("Could not initialize " + object, e);
			}
		}
	}

	private <T> ItemReader<T> reader(ItemReader<T> reader) {
		initializeBean(reader);
		if (reader instanceof RedisItemReader) {
			return reader;
		}
		if (threads > 1 && reader instanceof ItemStreamReader) {
			SynchronizedItemStreamReader<T> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate((ItemStreamReader<T>) reader);
			return synchronizedReader;
		}
		return reader;
	}

	private <T> ItemWriter<T> writer(ItemWriter<T> writer) {
		if (dryRun) {
			return new NoopItemWriter<>();
		}
		initializeBean(writer);
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

}
