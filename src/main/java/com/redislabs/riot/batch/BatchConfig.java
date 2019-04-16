package com.redislabs.riot.batch;

import java.util.Map;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
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
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redislabs.riot.processor.SpelProcessor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchConfig implements BatchConfigurer, InitializingBean {

	private PlatformTransactionManager transactionManager;
	private JobRepository jobRepository;
	private JobLauncher jobLauncher;
	private JobExplorer jobExplorer;

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

	@Override
	public void afterPropertiesSet() throws Exception {
		if (this.transactionManager == null) {
			this.transactionManager = new ResourcelessTransactionManager();
		}
		MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(transactionManager);
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

	public Step step(BatchOptions options, ItemStreamReader<Map<String, Object>> reader, SpelProcessor processor,
			ItemWriter<Map<String, Object>> writer) {
		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
		SimpleStepBuilder<Map<String, Object>, Map<String, Object>> builder = stepBuilderFactory.get("import-step")
				.<Map<String, Object>, Map<String, Object>>chunk(options.getChunkSize());
		builder.listener(new StepExecutionListener() {

			@Override
			public void beforeStep(StepExecution stepExecution) {
				log.debug("Starting step {}", stepExecution.getStepName());
			}

			@Override
			public ExitStatus afterStep(StepExecution stepExecution) {
				log.debug("Finished step {} - wrote {} items", stepExecution.getStepName(),
						stepExecution.getWriteCount());
				return null;
			}
		});
		if (options.getSleep() > 0) {
			builder.reader(new ThrottledItemStreamReader<Map<String, Object>>(reader, options.getSleep(), 0));
		} else {
			builder.reader(reader);
		}
		if (processor != null) {
			builder.processor(processor);
			builder.listener(processor);
		}
		builder.writer(writer);
		TaskletStep taskletStep = builder.build();
		if (options.getPartitions() > 1) {
			IndexedPartitioner partitioner = new IndexedPartitioner(options.getPartitions());
			return stepBuilderFactory.get("import-partitioner-step").partitioner("import-delegate-step", partitioner)
					.step(taskletStep).taskExecutor(new SimpleAsyncTaskExecutor()).build();
		}
		return taskletStep;

	}

	public Job importJob(BatchOptions options, ItemStreamReader<Map<String, Object>> reader, SpelProcessor processor,
			ItemWriter<Map<String, Object>> writer) {
		JobBuilderFactory jobBuilderFactory = new JobBuilderFactory(jobRepository);
		Step step = step(options, reader, processor, writer);
		return jobBuilderFactory.get("import-job").start(step).build();
	}

	public JobExecution launch(Job job) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		return jobLauncher.run(job, new JobParameters());
	}

}
