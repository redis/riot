package com.redislabs.riot.batch;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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

	private Step step() throws Exception {
		StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository(), transactionManager);
		SimpleStepBuilder<I, O> builder = stepBuilderFactory.get("import-step").<I, O>chunk(chunkSize);
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
		if (sleep == null) {
			builder.reader(reader);
		} else {
			builder.reader(new ThrottledItemStreamReader<I>(reader, sleep, 0));
		}
		if (processor != null) {
			builder.processor(processor);
			builder.listener(processor);
		}
		builder.writer(writer);
		TaskletStep taskletStep = builder.build();
		if (partitions > 1) {
			IndexedPartitioner partitioner = new IndexedPartitioner(partitions);
			return stepBuilderFactory.get("import-partitioner-step").partitioner("import-delegate-step", partitioner)
					.step(taskletStep).taskExecutor(new SimpleAsyncTaskExecutor()).build();
		}
		return taskletStep;

	}

	public Job build() throws Exception {
		return jobBuilderFactory().get("riot-job").start(step()).build();
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
