package com.redislabs.riot.cli;

import java.text.NumberFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.batch.JobExecutor;
import com.redislabs.riot.batch.StepThread;
import com.redislabs.riot.batch.ThrottlingItemReader;
import com.redislabs.riot.batch.ThrottlingItemStreamReader;
import com.redislabs.riot.batch.Transfer;
import com.redislabs.riot.batch.TransferContext;

import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
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
	@ArgGroup(exclusive = false, heading = "Transfer options%n")
	private TransferOptions options = new TransferOptions();

	protected void execute(Transfer<I, O> transfer) {
		List<StepThread<I, O>> stepThreads = new ArrayList<>();
		for (int thread = 0; thread < options.threads(); thread++) {
			TransferContext context = new TransferContext(thread, options.threads());
			ItemReader<I> reader;
			try {
				reader = configure(transfer.reader(context));
			} catch (Exception e) {
				log.error("Could not initialize reader", e);
				continue;
			}
			ItemProcessor<I, O> processor;
			try {
				processor = transfer.processor(context);
			} catch (Exception e) {
				log.error("Could not initialize processor", e);
				continue;
			}
			ItemWriter<O> writer;
			try {
				writer = transfer.writer(context);
			} catch (Exception e) {
				log.error("Could not initialize writer", e);
				continue;
			}
			stepThreads.add(new StepThread<>(thread, reader, processor, writer, options.batchSize()));
		}
		ExecutionContext executionContext = new ExecutionContext();
		stepThreads.forEach(t -> t.open(executionContext));
		ExecutorService executor = Executors.newFixedThreadPool(options.threads());
		stepThreads.forEach(t -> executor.execute(t));
		executor.shutdown();
		ProgressBarBuilder pbb = new ProgressBarBuilder().setInitialMax(options.count() == null ? -1 : options.count())
				.setTaskName(transfer.taskName()).showSpeed();
		if (options.showUnit()) {
			pbb.setUnit(" " + transfer.unitName() + "s", 1);
		}
		ProgressBar pb = pbb.build();
		while (!executor.isTerminated()) {
			long writeCount = 0;
			int nRunningThreads = 0;
			for (StepThread<I, O> stepThread : stepThreads) {
				writeCount += stepThread.writeCount();
				if (stepThread.running()) {
					nRunningThreads++;
				}
			}
			pb.stepTo(writeCount);
			pb.setExtraMessage(" (" + nRunningThreads + " threads)");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				log.error("Interrupted", e);
			}
		}
		stepThreads.forEach(t -> t.close());
	}

	protected void oldExecute(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		JobExecutor executor;
		try {
			executor = new JobExecutor();
		} catch (Exception e) {
			log.error("Could not initialize Spring Batch job executor", e);
			return;
		}
		log.info("Transferring from {} to {}", reader, writer);
		JobExecution execution;
		try {
			execution = executor.execute(spec.name() + "-step", configure(reader), processor, writer,
					options.batchSize(), options.threads(), false);
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
			log.info("Transferred {} items in {} seconds ({} ops/sec)", numberFormat.format(writeCount),
					duration.get(ChronoUnit.SECONDS), numberFormat.format(throughput));
		}
	}

	private ItemReader<I> configure(ItemReader<I> reader) {
		if (options.count() != null) {
			if (reader instanceof AbstractItemCountingItemStreamItemReader) {
				((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(options.count());
			} else {
				log.warn("Count is set for a source that does not support capping");
			}
		}
		if (options.sleep() == null) {
			return reader;
		}
		if (reader instanceof ItemStreamReader) {
			return new ThrottlingItemStreamReader<I>((ItemStreamReader<I>) reader, options.sleep());
		}
		return new ThrottlingItemReader<I>(reader, options.sleep());
	}

}
