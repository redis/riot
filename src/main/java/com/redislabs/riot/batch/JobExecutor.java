package com.redislabs.riot.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;

public class JobExecutor {

	private ResourcelessTransactionManager transactionManager;
	private MapJobRepositoryFactoryBean jobRepositoryFactory;
	private JobRepository jobRepository;
	private JobBuilderFactory jobFactory;
	private StepBuilderFactory stepFactory;
	private SimpleJobLauncher jobLauncher;

	public JobExecutor() throws Exception {
		this.transactionManager = new ResourcelessTransactionManager();
		this.jobRepositoryFactory = new MapJobRepositoryFactoryBean(transactionManager);
		this.jobRepositoryFactory.afterPropertiesSet();
		this.jobRepository = jobRepositoryFactory.getObject();
		this.jobFactory = new JobBuilderFactory(jobRepository);
		this.stepFactory = new StepBuilderFactory(jobRepository, transactionManager);
		this.jobLauncher = new SimpleJobLauncher();
		this.jobLauncher.setJobRepository(jobRepository);
		this.jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		this.jobLauncher.afterPropertiesSet();
	}

	public <I, O> JobExecution execute(String name, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer, int threads, int chunkSize) throws Exception {
		SimpleStepBuilder<I, O> builder = stepFactory.get(name).<I, O>chunk(chunkSize);
		builder.reader(reader);
		if (processor != null) {
			builder.processor(processor);
		}
		builder.writer(writer);
		Step step = builder.build();
		if (threads > 1) {
			step = stepFactory.get(name + "-partitioner").partitioner(name, new IndexedPartitioner(threads)).step(step)
					.taskExecutor(new SimpleAsyncTaskExecutor()).build();
		}
		Job job = jobFactory.get(name).start(step).build();
		return jobLauncher.run(job, new JobParameters());
	}
}
