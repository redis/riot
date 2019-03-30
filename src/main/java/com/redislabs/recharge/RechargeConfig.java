package com.redislabs.recharge;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redislabs.recharge.processor.SpelProcessor;
import com.redislabs.recharge.redis.writer.AbstractRedisWriter;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class, BatchAutoConfiguration.class })
@EnableConfigurationProperties(RechargeProperties.class)
@EnableBatchProcessing
@Slf4j
public class RechargeConfig implements BatchConfigurer {

	private PlatformTransactionManager transactionManager;
	private JobRepository jobRepository;
	private JobLauncher jobLauncher;
	private JobExplorer jobExplorer;

	@Autowired
	private Job importJob;

	@Bean
	@Override
	public JobRepository getJobRepository() {
		return jobRepository;
	}

	@Bean
	@Override
	public PlatformTransactionManager getTransactionManager() {
		return transactionManager;
	}

	@Bean
	@Override
	public JobLauncher getJobLauncher() {
		return jobLauncher;
	}

	@Bean
	@Override
	public JobExplorer getJobExplorer() {
		return jobExplorer;
	}

	@PostConstruct
	void initialize() throws Exception {
		if (this.transactionManager == null) {
			this.transactionManager = new ResourcelessTransactionManager();
		}
		MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(this.transactionManager);
		jobRepositoryFactory.afterPropertiesSet();
		this.jobRepository = jobRepositoryFactory.getObject();
		MapJobExplorerFactoryBean jobExplorerFactory = new MapJobExplorerFactoryBean(jobRepositoryFactory);
		jobExplorerFactory.afterPropertiesSet();
		this.jobExplorer = jobExplorerFactory.getObject();
		this.jobLauncher = createJobLauncher();
	}

	private JobLauncher createJobLauncher() throws Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		jobLauncher.afterPropertiesSet();
		return jobLauncher;
	}

	@Bean
	public Job importJob(JobBuilderFactory jobBuilderFactory, Step step) throws RechargeException {
		return jobBuilderFactory.get("import-job").start(step).build();
	}

	@Bean
	public Step step(RechargeProperties props, StepBuilderFactory stepBuilderFactory, TaskletStep taskletStep)
			throws RechargeException {
		if (props.getPartitions() > 1) {
			IndexedPartitioner partitioner = new IndexedPartitioner(props.getPartitions());
			return stepBuilderFactory.get("import-partitioner-step").partitioner("import-delegate-step", partitioner)
					.step(taskletStep).taskExecutor(new SimpleAsyncTaskExecutor()).build();
		}
		return taskletStep;
	}

	@Bean
	public TaskletStep tasklet(RechargeProperties props, StepBuilderFactory stepBuilderFactory,
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader, SpelProcessor processor,
			AbstractRedisWriter writer) throws RechargeException {
		SimpleStepBuilder<Map<String, Object>, Map<String, Object>> builder = stepBuilderFactory.get("import-step")
				.<Map<String, Object>, Map<String, Object>>chunk(props.getChunkSize());
		builder.listener(new StepExecutionListener() {

			@Override
			public void beforeStep(StepExecution stepExecution) {
				// TODO Auto-generated method stub

			}

			@Override
			public ExitStatus afterStep(StepExecution stepExecution) {
				log.info("Wrote {} items", stepExecution.getWriteCount());
				return null;
			}
		});
		if (props.getSleep() > 0 || props.getSleepNanos() > 0) {
			builder.reader(new ThrottledItemStreamReader<Map<String, Object>>(reader, props.getSleep(),
					props.getSleepNanos()));
		} else {
			builder.reader(reader);
		}
		if (!processor.isPassthrough()) {
			builder.processor(processor);
			builder.listener(processor);
		}
		builder.writer(writer);
		return builder.build();
	}

	public void runImport() throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		jobLauncher.run(importJob, new JobParameters());
	}

}
