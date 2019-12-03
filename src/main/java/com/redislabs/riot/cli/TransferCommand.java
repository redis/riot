package com.redislabs.riot.cli;

import java.text.NumberFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

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

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Slf4j
@Accessors(fluent = true)
public abstract class TransferCommand<I, O> extends RiotCommand {

	public final static String PARTITION = "partition";
	public final static String PARTITIONS = "partitions";

	@Spec
	private CommandSpec spec;
	@Setter
	@ArgGroup(exclusive = false, heading = "Transfer options%n")
	private TransferOptions transfer = new TransferOptions();

	protected void execute(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		JobExecutor executor;
		try {
			executor = new JobExecutor();
		} catch (Exception e) {
			log.error("Could not initialize Spring Batch job executor", e);
			return;
		}
		JobExecution execution;
		try {
			execution = executor.execute(spec.name() + "-step", configure(reader), processor, writer,
					transfer.batchSize(), transfer.threads(), partitioned());
		} catch (Exception e) {
			log.error("Could not execute {}", spec.name(), e);
			return;
		}
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
	}

	protected boolean partitioned() {
		return false;
	}

	private ItemReader<I> configure(ItemReader<I> reader) {
		if (transfer.count() != null) {
			if (reader instanceof AbstractItemCountingItemStreamItemReader) {
				((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(transfer.count());
			} else {
				log.warn("Count is set for a source that does not support capping");
			}
		}
		if (transfer.sleep() == null) {
			return reader;
		}
		if (reader instanceof ItemStreamReader) {
			return new ThrottlingItemStreamReader<I>((ItemStreamReader<I>) reader, transfer.sleep());
		}
		return new ThrottlingItemReader<I>(reader, transfer.sleep());
	}

}
