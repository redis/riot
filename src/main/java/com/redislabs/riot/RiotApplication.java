package com.redislabs.riot;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.Locale;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redislabs.riot.cli.AbstractCommand;
import com.redislabs.riot.cli.DatabaseReaderCommand;
import com.redislabs.riot.cli.FakerGeneratorReaderCommand;
import com.redislabs.riot.cli.GeneratorReaderHelpCommand;
import com.redislabs.riot.cli.RedisReaderCommand;
import com.redislabs.riot.cli.SimpleGeneratorReaderCommand;
import com.redislabs.riot.cli.file.DelimitedFileReaderCommand;
import com.redislabs.riot.cli.file.FixedLengthFileReaderCommand;
import com.redislabs.riot.cli.file.JsonFileReaderCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "riot", subcommands = { DelimitedFileReaderCommand.class, FixedLengthFileReaderCommand.class,
		JsonFileReaderCommand.class, DatabaseReaderCommand.class, FakerGeneratorReaderCommand.class,
		GeneratorReaderHelpCommand.class, SimpleGeneratorReaderCommand.class,
		RedisReaderCommand.class }, synopsisSubcommandLabel = "[SOURCE]", commandListHeading = "Sources:%n")
public class RiotApplication extends AbstractCommand {

	static {
		LogManager.getLogManager().getLogger(Logger.GLOBAL_LOGGER_NAME).setLevel(Level.WARNING);
	}

	/**
	 * Just to avoid picocli complain in Eclipse console
	 */
	@Option(names = "--spring.output.ansi.enabled", hidden = true)
	private String ansiEnabled;
	@Option(names = "--threads", description = "Number of processing threads.", paramLabel = "<count>")
	private int threads = 1;
	@Option(names = "--batch", description = "Number of items in each batch.", paramLabel = "<size>")
	private int batchSize = 50;

	private NumberFormat numberFormat = NumberFormat.getIntegerInstance();

	public static void main(String[] args) throws IOException {
		setLevel(Level.WARNING);
		BufferedReader in = new BufferedReader(
				new InputStreamReader(ClassLoader.getSystemResource("banner.txt").openStream()));
		String line;
		while ((line = in.readLine()) != null) {
			System.out.println(line);
		}
		in.close();
		CommandLine commandLine = new CommandLine(new RiotApplication());
		commandLine.registerConverter(Locale.class, s -> new Locale.Builder().setLanguageTag(s).build());
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		commandLine.execute(args);
	}

	private static void setLevel(Level targetLevel) {
		Logger root = Logger.getLogger("");
		root.setLevel(targetLevel);
		for (Handler handler : root.getHandlers()) {
			handler.setLevel(targetLevel);
		}
	}

	public <I, O> void execute(ItemStreamReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws Exception {
		PlatformTransactionManager transactionManager = new ResourcelessTransactionManager();
		MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(transactionManager);
		jobRepositoryFactory.afterPropertiesSet();
		JobRepository jobRepository = jobRepositoryFactory.getObject();
		JobBuilderFactory jobFactory = new JobBuilderFactory(jobRepository);
		StepBuilderFactory stepFactory = new StepBuilderFactory(jobRepository, transactionManager);
		SimpleStepBuilder<I, O> builder = stepFactory.get("tasklet-step").<I, O>chunk(batchSize);
		builder.reader(reader);
		if (processor != null) {
			builder.processor(processor);
		}
		builder.writer(writer);
		TaskletStep taskletStep = builder.build();
		Step step = taskletStep;
		if (threads > 1) {
			step = stepFactory.get("partitioner-step").partitioner("delegate-step", new IndexedPartitioner(threads))
					.step(taskletStep).taskExecutor(new SimpleAsyncTaskExecutor()).build();
		}
		Job job = jobFactory.get("riot-job").start(step).build();
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
			System.out.println("Wrote " + numberFormat.format(writeCount) + " items in " + duration.toSeconds()
					+ " seconds (" + numberFormat.format(throughput) + " items/sec)");
		}
	}

}
