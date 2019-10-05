package com.redislabs.riot.cli;

import java.text.NumberFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.batch.JobExecutor;
import com.redislabs.riot.batch.ThrottlingItemReader;
import com.redislabs.riot.batch.ThrottlingItemStreamReader;

import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class TransferCommand extends AbstractCommand {

	private final Logger log = LoggerFactory.getLogger(TransferCommand.class);

	@Spec
	private CommandSpec spec;

	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int threads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batchSize = 50;
	@Option(names = { "-m", "--max" }, description = "Max number of items to read", paramLabel = "<count>")
	private Integer count;
	@Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<ms>")
	private Long sleep;

	private ItemReader throttle(ItemReader reader) {
		if (count != null) {
			if (reader instanceof AbstractItemCountingItemStreamItemReader) {
				((AbstractItemCountingItemStreamItemReader) reader).setMaxItemCount(count);
			} else {
				log.warn("Count is set for a source that does not support capping");
			}
		}
		if (sleep == null) {
			return reader;
		}
		if (reader instanceof ItemStreamReader) {
			return new ThrottlingItemStreamReader((ItemStreamReader) reader, sleep);
		}
		return new ThrottlingItemReader(reader, sleep);
	}

	public JobExecution execute(String name, ItemReader reader, ItemProcessor processor, ItemWriter writer)
			throws Exception {
		JobExecutor executor = new JobExecutor();
		log.info("Executing {}, threads: {}, batch size: {}", name, threads, batchSize);
		return executor.execute(name, throttle(reader), processor, writer, threads, batchSize);

	}

	@Override
	public void run() {
		ItemReader reader;
		try {
			reader = reader();
		} catch (Exception e) {
			log.error("Could not initialize {} reader", spec.name(), e);
			return;
		}
		ItemWriter writer;
		try {
			writer = writer();
		} catch (Exception e) {
			log.error("Could not initialize {} writer", spec.name(), e);
			return;
		}
		ItemProcessor processor;
		try {
			processor = processor();
		} catch (Exception e) {
			log.error("Could not initialize {} processor", spec.name(), e);
			return;
		}
		try {
			JobExecution execution = execute(spec.name(), reader, processor, writer);
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

	protected abstract ItemReader reader() throws Exception;

	protected abstract ItemProcessor processor() throws Exception;

	protected abstract ItemWriter writer() throws Exception;

}
