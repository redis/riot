package com.redislabs.riot.cli;

import java.text.NumberFormat;
import java.time.Duration;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redislabs.riot.BaseCommand;
import com.redislabs.riot.RiotApplication;
import com.redislabs.riot.batch.JobBuilder;

import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command
public class JobCommand<I, O> extends BaseCommand {

	@ParentCommand
	@Getter
	private RiotApplication parent;

	private NumberFormat numberFormat = NumberFormat.getIntegerInstance();

	@Option(names = "--threads", description = "Number of partitions to use for processing. (default: ${DEFAULT-VALUE}).")
	private int threads = 1;
	@Option(names = "--chunk-size", description = "The chunk size commit interval. (default: ${DEFAULT-VALUE}).")
	private int chunkSize = JobBuilder.DEFAULT_CHUNK_SIZE;
	@Option(names = "--sleep", description = "Sleep duration in milliseconds between each read.")
	private Long sleep;

	public void execute(ItemStreamReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		try {
			PlatformTransactionManager transactionManager = new ResourcelessTransactionManager();
			MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(transactionManager);
			jobRepositoryFactory.afterPropertiesSet();
			JobRepository jobRepository = jobRepositoryFactory.getObject();
			JobBuilderFactory jobBuilderFactory = new JobBuilderFactory(jobRepository);
			StepBuilderFactory stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
			JobBuilder<I, O> builder = new JobBuilder<>(jobBuilderFactory, stepBuilderFactory);
			builder.setChunkSize(chunkSize);
			builder.setReader(reader);
			builder.setProcessor(processor);
			builder.setWriter(writer);
			builder.setPartitions(threads);
			builder.setSleep(sleep);
			Job job = builder.build();
			SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
			jobLauncher.setJobRepository(jobRepository);
			jobLauncher.setTaskExecutor(new SyncTaskExecutor());
			jobLauncher.afterPropertiesSet();
			JobExecution execution = jobLauncher.run(job, new JobParameters());
			if (execution.getExitStatus().equals(ExitStatus.FAILED)) {
				execution.getAllFailureExceptions().forEach(e -> e.printStackTrace());
			}
			for (StepExecution stepExecution : execution.getStepExecutions()) {
				Duration duration = Duration
						.ofMillis(stepExecution.getEndTime().getTime() - stepExecution.getStartTime().getTime());
				int writeCount = stepExecution.getWriteCount();
				double throughput = (double) writeCount / duration.toMillis() * 1000;
				System.out.println(stepExecution.getStepName() + ": " + numberFormat.format(writeCount) + " items in "
						+ duration.toSeconds() + " seconds (" + numberFormat.format(throughput) + " items/sec)");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
