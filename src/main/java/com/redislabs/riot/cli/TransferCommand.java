package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.CappedItemReader;
import com.redislabs.riot.ThrottlingItemReader;
import com.redislabs.riot.ThrottlingItemStreamReader;
import com.redislabs.riot.Transfer;
import com.redislabs.riot.TransferExecution;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Slf4j
public abstract class TransferCommand<I, O> extends RiotCommand {

	@Spec
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

	protected void execute(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		Transfer<I, O> transfer = new Transfer<>();
		transfer.setBatchSize(batchSize);
		transfer.setNThreads(nThreads);
		transfer.setReader(throttle(cap(reader)));
		transfer.setProcessor(processor);
		transfer.setWriter(writer);
		TransferExecution<I, O> execution = transfer.execute();
		ProgressReporter reporter = progressReporter();
		reporter.start();
		while (!execution.isFinished()) {
			reporter.onUpdate(execution.progress());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				log.error("Interrupted", e);
			}
		}
		reporter.onUpdate(execution.progress());
		reporter.stop();
		close(reader, writer);
	}

	protected void close(ItemReader<I> reader, ItemWriter<O> writer) {
		if (reader instanceof ItemStream) {
			((ItemStream) reader).close();
		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).close();
		}
	}

	private ItemReader<I> throttle(ItemReader<I> reader) {
		if (sleep == null) {
			return reader;
		}
		if (reader instanceof ItemStreamReader) {
			return new ThrottlingItemStreamReader<I>((ItemStreamReader<I>) reader, sleep);
		}
		return new ThrottlingItemReader<I>(reader, sleep);
	}

	private ItemReader<I> cap(ItemReader<I> reader) {
		if (maxItemCount == null) {
			return reader;
		}
		return new CappedItemReader<I>(reader, maxItemCount);
	}

	private ProgressReporter progressReporter() {
		if (parent().getOptions().isQuiet()) {
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
