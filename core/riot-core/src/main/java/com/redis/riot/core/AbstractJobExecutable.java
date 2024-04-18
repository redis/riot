package com.redis.riot.core;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.JobFactory;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;

public abstract class AbstractJobExecutable implements InitializingBean, AutoCloseable {

	private JobFactory jobFactory;
	private String name;

	private StepOptions stepOptions = new StepOptions();
	private List<StepListener> stepListeners = new ArrayList<>();

	protected AbstractJobExecutable() {
		setName(ClassUtils.getShortName(getClass()));
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (jobFactory == null) {
			jobFactory = new JobFactory();
		}
		jobFactory.afterPropertiesSet();
	}

	@Override
	public void close() throws Exception {
		jobFactory = null;
	}

	private <O> ItemWriter<O> writer(ItemWriter<O> writer) {
		if (stepOptions.isDryRun()) {
			return new NoopItemWriter<>();
		}
		if (RiotUtils.isPositive(stepOptions.getSleep())) {
			return new ThrottledItemWriter<>(writer, stepOptions.getSleep());
		}
		return writer;
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant();
		ftStep.retryLimit(stepOptions.getRetryLimit());
		ftStep.skipLimit(stepOptions.getSkipLimit());
		ftStep.skip(RedisCommandExecutionException.class);
		ftStep.skip(ParseException.class);
		ftStep.noSkip(RedisCommandTimeoutException.class);
		ftStep.retry(RedisCommandTimeoutException.class);
		ftStep.noRetry(RedisCommandExecutionException.class);
		ftStep.noRetry(ParseException.class);
		return ftStep;
	}

	private <I> ItemReader<I> reader(ItemReader<I> reader) {
		if (reader instanceof RedisItemReader) {
			return reader;
		}
		if (stepOptions.getThreads() > 1) {
			if (reader instanceof ItemStreamReader) {
				SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
				synchronizedReader.setDelegate((ItemStreamReader<I>) reader);
				return synchronizedReader;
			}
			return new SynchronizedItemReader<>(reader);
		}
		return reader;
	}

	public JobExecution execute() throws JobExecutionException {
		JobExecution jobExecution = jobFactory.run(job());
		return JobFactory.checkJobExecution(jobExecution);
	}

	protected abstract Job job();

	protected <I, O> FaultTolerantStepBuilder<I, O> step(ItemReader<I> reader, ItemWriter<O> writer) {
		return step(name, reader, writer);
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> step(String name, ItemReader<I> reader, ItemWriter<O> writer) {
		SimpleStepBuilder<I, O> step = jobFactory.step(name, stepOptions.getChunkSize());
		step.reader(reader(reader));
		step.writer(writer(writer));
		if (stepOptions.getThreads() > 1) {
			step.taskExecutor(JobFactory.threadPoolTaskExecutor(stepOptions.getThreads()));
		}
		stepListeners.forEach(s -> s.step(step, name, reader, writer));
		return faultTolerant(step);
	}

	protected JobBuilder jobBuilder() {
		return jobFactory.job(name);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setJobFactory(JobFactory jobFactory) {
		this.jobFactory = jobFactory;
	}

	public void addStepListener(StepListener listener) {
		this.stepListeners.add(listener);
	}

	public StepOptions getStepOptions() {
		return stepOptions;
	}

	public void setStepOptions(StepOptions stepOptions) {
		this.stepOptions = stepOptions;
	}

	protected void configureRedisReader(RedisItemReader<?, ?, ?> reader) {
		reader.setJobFactory(jobFactory);
	}

}
