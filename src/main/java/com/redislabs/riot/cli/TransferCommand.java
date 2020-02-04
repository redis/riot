package com.redislabs.riot.cli;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.transfer.CappedReader;
import com.redislabs.riot.transfer.ErrorHandler;
import com.redislabs.riot.transfer.Flow;
import com.redislabs.riot.transfer.ThrottledReader;
import com.redislabs.riot.transfer.Transfer;
import com.redislabs.riot.transfer.TransferExecution;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Slf4j
@Command
public @Data abstract class TransferCommand<I, O> extends RiotCommand {

	@Spec
	@Getter(AccessLevel.NONE)
	@Setter(AccessLevel.NONE)
	private CommandSpec spec;
	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int nThreads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batchSize = 50;
	@Option(names = { "-m", "--max" }, description = "Max number of items to read", paramLabel = "<count>")
	private Long maxItemCount;
	@Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<ms>")
	private Long sleep;
	@Option(names = "--progress", description = "Progress reporting interval (default: ${DEFAULT-VALUE} ms)", paramLabel = "<ms>")
	private long progressRate = 300;
	@Option(names = "--transfer-max-wait", description = "Max duration to wait for transfer to complete", paramLabel = "<ms>")
	private Long maxWait;

	protected Transfer transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		Transfer transfer = new Transfer();
		transfer.flow(flow("main", reader, processor, writer));
		return transfer;
	}

	protected ErrorHandler errorHandler() {
		return e -> log.error("Could not read item", e);
	}

	protected Flow flow(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return Flow.builder().name(name).batchSize(batchSize).nThreads(nThreads).reader(throttle(cap(reader)))
				.processor(processor).writer(writer).errorHandler(errorHandler()).build();
	}

	protected void execute(Transfer transfer) {
		ProgressReporter reporter = progressReporter();
		reporter.start();
		TransferExecution execution = transfer.execute();
		ScheduledExecutorService progressReportExecutor = Executors.newSingleThreadScheduledExecutor();
		progressReportExecutor.scheduleAtFixedRate(() -> reporter.onUpdate(execution.progress()), 0, progressRate,
				TimeUnit.MILLISECONDS);
		execution.awaitTermination(maxWait(), TimeUnit.MILLISECONDS);
		progressReportExecutor.shutdown();
		reporter.onUpdate(execution.progress());
		reporter.stop();
	}

	private long maxWait() {
		if (maxWait == null) {
			return Long.MAX_VALUE;
		}
		return maxWait;
	}

	private ItemReader<I> throttle(ItemReader<I> reader) {
		if (sleep == null) {
			return reader;
		}
		return new ThrottledReader<I>().reader(reader).sleep(sleep);
	}

	private ItemReader<I> cap(ItemReader<I> reader) {
		if (maxItemCount == null) {
			return reader;
		}
		return new CappedReader<I>(reader, maxItemCount);
	}

	private ProgressReporter progressReporter() {
		if (parent().options().quiet()) {
			return new NoopProgressReporter();
		}
		ProgressBarReporter progressBarReporter = new ProgressBarReporter().taskName(taskName());
		if (maxItemCount != null) {
			progressBarReporter.initialMax(maxItemCount);
		}
		return progressBarReporter;
	}

	protected abstract String taskName();

}
