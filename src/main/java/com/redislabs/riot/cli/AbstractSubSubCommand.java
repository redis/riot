package com.redislabs.riot.cli;

import java.io.IOException;
import java.text.NumberFormat;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.task.SyncTaskExecutor;

import com.redislabs.riot.batch.JobBuilder;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractSubSubCommand<I, O> extends HelpAwareCommand {

	@ParentCommand
	private AbstractSubCommand<I, O> parent;

	@Override
	public Void call() throws Exception {
		JobBuilder<I, O> builder = new JobBuilder<>();
		builder.setChunkSize(parent.getParent().getChunkSize());
		AbstractItemCountingItemStreamItemReader<I> reader = reader();
		if (parent.getParent().getMaxCount() != null) {
			reader.setMaxItemCount(parent.getParent().getMaxCount());
		}
		builder.setReader(reader);
		builder.setWriter(writer());
		builder.setProcessor(processor());
		builder.setPartitions(parent.getParent().getThreads());
		builder.setSleep(parent.getParent().getSleep());
		Job job = builder.build();
		long startTime = System.currentTimeMillis();
		System.out.println("Importing into " + getTargetDescription() + " from " + parent.getSourceDescription());
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(builder.jobRepository());
		jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		jobLauncher.afterPropertiesSet();
		JobExecution execution = jobLauncher.run(job, new JobParameters());
		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		NumberFormat numberFormat = NumberFormat.getIntegerInstance();
		for (StepExecution stepExecution : execution.getStepExecutions()) {
			double durationInSeconds = (double) duration / 1000;
			int writeCount = stepExecution.getWriteCount();
			double throughput = writeCount / durationInSeconds;
			System.out.println("Imported " + numberFormat.format(writeCount) + " items in " + durationInSeconds
					+ " seconds (" + numberFormat.format(throughput) + " writes/sec)");
		}
		return null;
	}

	protected abstract String getTargetDescription();

	protected abstract ItemStreamWriter<O> writer() throws Exception;

	protected ItemProcessor<I, O> processor() {
		return null;
	}

	protected abstract AbstractItemCountingItemStreamItemReader<I> reader() throws IOException;

}
