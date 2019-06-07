package com.redislabs.riot.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

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
	private ItemWriter<O> writer;
	@Setter
	private ItemProcessor<I, O> processor;
	private JobBuilderFactory jobBuilderFactory;
	private StepBuilderFactory stepBuilderFactory;

	public JobBuilder(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
	}

	private TaskletStep taskletStep() throws Exception {
		SimpleStepBuilder<I, O> builder = stepBuilderFactory.get("tasklet-step").<I, O>chunk(chunkSize);
		builder.reader(reader());
		if (processor != null) {
			builder.processor(processor);
			builder.listener(processor);
		}
		builder.writer(writer);
		return builder.build();
	}

	private ItemReader<? extends I> reader() {
		if (sleep == null) {
			return reader;
		}
		return new ThrottledItemStreamReader<I>(reader, sleep, 0);
	}

	public Job build() throws Exception {
		TaskletStep taskletStep = taskletStep();
		return jobBuilderFactory.get("riot-job").start(step(taskletStep)).build();
	}

	private Step step(TaskletStep taskletStep) throws Exception {
		if (partitions == 1) {
			return taskletStep;
		}
		IndexedPartitioner indexedPartitioner = new IndexedPartitioner(partitions);
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		return stepBuilderFactory.get("partitioner-step").partitioner("delegate-step", indexedPartitioner)
				.step(taskletStep).taskExecutor(taskExecutor).build();
	}

}
