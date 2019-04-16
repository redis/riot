package com.redislabs.riot.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.Setter;

public class JobBuilder<I, O> {

	public static final int DEFAULT_PARTITIONS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;

	@Setter
	private int partitions = DEFAULT_PARTITIONS;
	@Setter
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	@Setter
	private Long sleep;
	@Setter
	private ItemStreamReader<I> reader;
	@Setter
	private ItemStreamWriter<O> writer;
	@Setter
	private ItemProcessor<I, O> processor;

	private PlatformTransactionManager transactionManager = new ResourcelessTransactionManager();
	private JobBuilderFactory jobBuilderFactory;
	private MapJobRepositoryFactoryBean jobRepositoryFactory;
	private JobRepository jobRepository;
	private StepBuilderFactory stepBuilderFactory;

	private TaskletStep taskletStep() throws Exception {
		SimpleStepBuilder<I, O> builder = stepBuilderFactory().get("tasklet-step").<I, O>chunk(chunkSize);
		builder.reader(reader());
		if (processor != null) {
			builder.processor(processor);
			builder.listener(processor);
		}
		builder.writer(writer);
		return builder.build();
	}

	private StepBuilderFactory stepBuilderFactory() throws Exception {
		if (stepBuilderFactory == null) {
			stepBuilderFactory = new StepBuilderFactory(jobRepository(), transactionManager);
		}
		return stepBuilderFactory;
	}

	private ItemReader<? extends I> reader() {
		if (sleep == null) {
			return reader;
		}
		return new ThrottledItemStreamReader<I>(reader, sleep, 0);
	}

	public Job build() throws Exception {
		TaskletStep taskletStep = taskletStep();
		return jobBuilderFactory().get("riot-job").start(step(taskletStep)).build();
	}

	private Step step(TaskletStep taskletStep) throws Exception {
		if (partitions == 1) {
			return taskletStep;
		}
		IndexedPartitioner indexedPartitioner = new IndexedPartitioner(partitions);
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		return stepBuilderFactory().get("partitioner-step").partitioner("delegate-step", indexedPartitioner)
				.step(taskletStep).taskExecutor(taskExecutor).build();
	}

	public JobBuilderFactory jobBuilderFactory() throws Exception {
		if (jobBuilderFactory == null) {
			jobBuilderFactory = new JobBuilderFactory(jobRepository());
		}
		return jobBuilderFactory;
	}

	public JobRepository jobRepository() throws Exception {
		if (jobRepository == null) {
			jobRepository = jobRepositoryFactory().getObject();
		}
		return jobRepository;
	}

	public MapJobRepositoryFactoryBean jobRepositoryFactory() throws Exception {
		if (jobRepositoryFactory == null) {
			jobRepositoryFactory = new MapJobRepositoryFactoryBean(transactionManager);
			jobRepositoryFactory.afterPropertiesSet();
		}
		return jobRepositoryFactory;
	}

}
