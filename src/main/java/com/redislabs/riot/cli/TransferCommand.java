package com.redislabs.riot.cli;

import java.text.NumberFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.batch.JobExecutor;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Slf4j
public abstract class TransferCommand extends AbstractCommand {

	@Setter
	@ArgGroup(exclusive = false, heading = "Transfer options%n")
	private TransferOptions transferOptions = new TransferOptions();

	public JobExecution execute(String name, ItemReader reader, ItemProcessor processor, ItemWriter writer)
			throws Exception {
		return new JobExecutor().execute(name, transferOptions.configure(reader), processor, writer,
				transferOptions.getThreads(), transferOptions.getBatchSize());

	}

	@Override
	public void execute(String name, RedisConnectionOptions redisOptions) {
		ItemReader reader;
		try {
			reader = reader(redisOptions);
		} catch (Exception e) {
			log.error("Could not initialize {} reader", name, e);
			return;
		}
		ItemWriter writer;
		try {
			writer = writer(redisOptions);
		} catch (Exception e) {
			log.error("Could not initialize {} writer", name, e);
			return;
		}
		ItemProcessor processor;
		try {
			processor = processor(redisOptions);
		} catch (Exception e) {
			log.error("Could not initialize {} processor", name, e);
			return;
		}
		try {
			JobExecution execution = execute(name, reader, processor, writer);
			if (execution.getExitStatus().getExitCode().equals(ExitStatus.FAILED.getExitCode())) {
				execution.getAllFailureExceptions().forEach(e -> e.printStackTrace());
			}
			StepExecution stepExecution = execution.getStepExecutions().iterator().next();
			if (stepExecution.getExitStatus().getExitCode().equals(ExitStatus.FAILED.getExitCode())) {
				stepExecution.getFailureExceptions()
						.forEach(e -> log.error("Could not execute step {}", stepExecution.getStepName(), e));
			} else {
				Duration duration = Duration
						.ofMillis(stepExecution.getEndTime().getTime() - stepExecution.getStartTime().getTime());
				int writeCount = stepExecution.getWriteCount();
				double throughput = (double) writeCount / duration.toMillis() * 1000;
				NumberFormat numberFormat = NumberFormat.getIntegerInstance();
				log.info("Wrote {} items in {} seconds ({} items/sec)", numberFormat.format(writeCount),
						duration.get(ChronoUnit.SECONDS), numberFormat.format(throughput));
			}
		} catch (Exception e) {
			log.error("Could not execute transfer", e);
		}
	}

	protected abstract ItemReader reader(RedisConnectionOptions options) throws Exception;

	protected abstract ItemProcessor processor(RedisConnectionOptions options) throws Exception;

	protected abstract ItemWriter writer(RedisConnectionOptions options) throws Exception;

}
