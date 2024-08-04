package com.redis.riot.core;

import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.JobUtils;
import com.redis.spring.batch.item.AbstractAsyncItemReader;

public class JobExecutionContext implements ExecutionContext {

	private String jobName;
	private String jobRepositoryName;
	private JobRepository jobRepository;
	private PlatformTransactionManager transactionManager;
	private JobLauncher jobLauncher;

	@Override
	public void afterPropertiesSet() throws Exception {
		if (jobRepository == null) {
			jobRepository = JobUtils.jobRepositoryFactoryBean(jobRepositoryName).getObject();
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

	public void configure(AbstractAsyncItemReader<?, ?> reader) {
		reader.setJobRepository(jobRepository);
	}

	public JobBuilder jobBuilder() {
		return new JobBuilder(jobName, jobRepository);
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
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

	public String getJobRepositoryName() {
		return jobRepositoryName;
	}

	public void setJobRepositoryName(String name) {
		this.jobRepositoryName = name;
	}

	@Override
	public void close() throws Exception {
		// do nothing
	}

	public <I, O> SimpleStepBuilder<I, O> step(String name, int chunkSize) {
		return new StepBuilder(name, jobRepository).<I, O>chunk(chunkSize, transactionManager);
	}

}
