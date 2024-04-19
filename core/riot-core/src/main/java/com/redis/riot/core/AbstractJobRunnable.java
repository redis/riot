package com.redis.riot.core;

import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.JobFactory;
import com.redis.spring.batch.operation.KeyValueWrite;
import com.redis.spring.batch.operation.KeyValueWrite.WriteMode;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;

public abstract class AbstractJobRunnable implements Runnable {

	public static final SkipPolicy DEFAULT_SKIP_POLICY = new NeverSkipItemSkipPolicy();
	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;
	public static final Duration DEFAULT_SLEEP = Duration.ZERO;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_THREADS = 1;

	private static final String FAILED_JOB_MESSAGE = "Error executing job %s";

	private String name;
	private List<StepConfiguration> stepConfigurations = new ArrayList<>();
	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private Duration sleep = DEFAULT_SLEEP;
	private boolean dryRun;
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private int retryLimit = DEFAULT_RETRY_LIMIT;
	private JobFactory jobFactory;

	protected AbstractJobRunnable() {
		setName(ClassUtils.getShortName(getClass()));
	}

	public JobFactory getJobFactory() {
		return jobFactory;
	}

	protected String name(String... suffixes) {
		List<String> elements = new ArrayList<>();
		elements.add(name);
		elements.addAll(Arrays.asList(suffixes));
		return String.join("-", elements);
	}

	public void addStepConfiguration(StepConfiguration configuration) {
		stepConfigurations.add(configuration);
	}

	public void setJobFactory(JobFactory jobFactory) {
		this.jobFactory = jobFactory;
	}

	@Override
	public void run() {
		if (jobFactory == null) {
			jobFactory = new JobFactory();
			try {
				jobFactory.afterPropertiesSet();
			} catch (Exception e) {
				throw new ExecutionException("Could not initialize job infrastructure", e);
			}
		}
		try {
			JobFactory.checkJobExecution(jobFactory.run(job()));
		} catch (JobExecutionException e) {
			throw new ExecutionException(String.format(FAILED_JOB_MESSAGE, name), e);
		}
	}

	protected JobBuilder jobBuilder() {
		return jobFactory.jobBuilder(name);
	}

	protected abstract Job job();

	protected void writer(RedisItemWriter<?, ?, ?> writer, RedisWriterOptions options) {
		writer.setMultiExec(options.isMultiExec());
		writer.setPoolSize(options.getPoolSize());
		writer.setWaitReplicas(options.getWaitReplicas());
		writer.setWaitTimeout(options.getWaitTimeout());
		if (writer.getOperation() instanceof KeyValueWrite) {
			KeyValueWrite<?, ?> operation = (KeyValueWrite<?, ?>) writer.getOperation();
			operation.setMode(options.isMerge() ? WriteMode.MERGE : WriteMode.OVERWRITE);
		}
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> step(String name, ItemReader<I> reader, ItemWriter<O> writer) {
		SimpleStepBuilder<I, O> builder = jobFactory.step(name, chunkSize);
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name(name, "reader"));
		}
		if (isMultiThreaded()) {
			builder.reader(synchronize(reader));
			builder.taskExecutor(JobFactory.threadPoolTaskExecutor(threads));
		} else {
			builder.reader(reader);
		}
		builder.writer(writer(writer));
		stepConfigurations.forEach(s -> s.configure(builder, name, reader, writer));
		return faultTolerant(builder);
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant();
		ftStep.skipLimit(skipLimit);
		ftStep.retryLimit(retryLimit);
		ftStep.skip(ParseException.class);
		ftStep.skip(RedisCommandExecutionException.class);
		ftStep.noRetry(ParseException.class);
		ftStep.noRetry(RedisCommandExecutionException.class);
		ftStep.noSkip(RedisCommandTimeoutException.class);
		ftStep.retry(RedisCommandTimeoutException.class);
		return ftStep;
	}

	private <T> ItemReader<T> synchronize(ItemReader<T> reader) {
		if (reader instanceof RedisItemReader) {
			return reader;
		}
		if (reader instanceof ItemStreamReader) {
			SynchronizedItemStreamReader<T> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate((ItemStreamReader<T>) reader);
			return synchronizedReader;
		}
		return new SynchronizedItemReader<>(reader);
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

}
