package com.redislabs.riot.cli;

import java.text.NumberFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.batch.JobExecutor;
import com.redislabs.riot.batch.Processor;
import com.redislabs.riot.batch.ThrottlingItemReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(usageHelpAutoWidth = true, abbreviateSynopsis = true)
public abstract class TransferCommand implements Runnable {

	private final Logger log = LoggerFactory.getLogger(TransferCommand.class);

	@Option(names = "--help", usageHelp = true, description = "Show this help message and exit")
	private boolean help;
	@Option(names = { "-t",
			"--threads" }, description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int threads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batchSize = 50;
	@Option(names = { "-n", "--max" }, description = "Max number of items to read", paramLabel = "<count>")
	private Integer count;
	@Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<millis>")
	private Long sleep;
	@Option(names = { "-c",
			"--processor" }, description = "SpEL expression to process a field", paramLabel = "<name=SpEL>")
	private Map<String, String> processorFields;

	@Override
	public void run() {
		try {
			ItemReader<Map<String, Object>> reader = throttle(reader());
			Processor processor = processor();
			try {
				ItemWriter<Map<String, Object>> writer = writer();
				JobExecutor executor = new JobExecutor();
				try {
					log.info("Transferring from {} to {}", sourceDescription(), targetDescription());
					JobExecution execution = executor.execute(reader, processor, writer, threads, batchSize);
					if (execution.getExitStatus().equals(ExitStatus.FAILED)) {
						execution.getAllFailureExceptions().forEach(e -> e.printStackTrace());
					}
					for (StepExecution stepExecution : execution.getStepExecutions()) {
						Duration duration = Duration.ofMillis(
								stepExecution.getEndTime().getTime() - stepExecution.getStartTime().getTime());
						int writeCount = stepExecution.getWriteCount();
						double throughput = (double) writeCount / duration.toMillis() * 1000;
						NumberFormat numberFormat = NumberFormat.getIntegerInstance();
						log.info("Wrote {} items in {} seconds ({} items/sec)", numberFormat.format(writeCount),
								duration.get(ChronoUnit.SECONDS), numberFormat.format(throughput));
					}
				} catch (Exception e) {
					log.error("Could not execute batch job", e);
				}
			} catch (Exception e) {
				log.error("Could not initialize writer for {}", targetDescription(), e);
			}
		} catch (Exception e) {
			log.error("Could not initialize reader for {}", sourceDescription(), e);
		}
	}

	private <T> ItemReader<T> throttle(ItemReader<T> reader) {
		if (count != null) {
			if (reader instanceof AbstractItemCountingItemStreamItemReader) {
				((AbstractItemCountingItemStreamItemReader<?>) reader).setMaxItemCount(count);
			} else {
				log.warn("Count is set for a source that does not support capping");
			}
		}
		if (sleep == null) {
			return reader;
		}
		return new ThrottlingItemReader<>(reader, sleep);
	}

	private Processor processor() throws Exception {
		if (processorFields == null) {
			return null;
		}
		return new Processor(processorFields);
	}

	protected abstract String sourceDescription();

	protected abstract ItemReader<Map<String, Object>> reader() throws Exception;

	protected abstract ItemWriter<Map<String, Object>> writer() throws Exception;

	protected abstract String targetDescription();
}
