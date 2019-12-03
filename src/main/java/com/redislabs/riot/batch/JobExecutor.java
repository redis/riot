package com.redislabs.riot.batch;

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
			ItemWriter<O> writer, int chunkSize, int nThreads, boolean partitioned) throws Exception {
		SimpleStepBuilder<I, O> builder = stepFactory.get(name).<I, O>chunk(chunkSize);
		builder.reader(reader);
		if (processor != null) {
			builder.processor(processor);
		}
		builder.writer(writer);
		return jobLauncher.run(jobFactory.get(name).start(step(name, builder, nThreads, partitioned)).build(),
				new JobParameters());
	}

	private <I, O> Step step(String name, SimpleStepBuilder<I, O> step, int nThreads, boolean partitioned) {
		if (partitioned) {
			return stepFactory.get(name + "-partitioner").partitioner(name, new IndexedPartitioner(nThreads))
					.step(step.build()).taskExecutor(new SimpleAsyncTaskExecutor()).build();
		}
		if (nThreads > 1) {
			SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
			taskExecutor.setConcurrencyLimit(nThreads);
			step.taskExecutor(taskExecutor);
			step.throttleLimit(nThreads);
		}
		return step.build();
	}
}
