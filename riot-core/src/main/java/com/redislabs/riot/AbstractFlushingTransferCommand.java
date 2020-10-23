package com.redislabs.riot;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.Transfer.ExecutionHook;

import picocli.CommandLine.Option;

public abstract class AbstractFlushingTransferCommand<I, O> extends AbstractTransferCommand<I, O> {

	@Option(names = "--flush-interval", description = "Duration between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long flushPeriod = 50;

	protected abstract boolean flushingEnabled();

	@Override
	protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		Transfer<I, O> transfer = super.transfer(reader, processor, writer);
		if (flushingEnabled()) {
			transfer.addExecutionHook(new FlushExecutionHook(transfer));
		}
		return transfer;
	}

	private class FlushExecutionHook implements ExecutionHook {

		private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		private final Transfer<I, O> transfer;
		private ScheduledFuture<?> future;

		public FlushExecutionHook(Transfer<I, O> transfer) {
			this.transfer = transfer;
		}

		@Override
		public void pre() {
			this.future = scheduler.scheduleAtFixedRate(transfer::flush, flushPeriod, flushPeriod,
					TimeUnit.MILLISECONDS);
		}

		@Override
		public void post() {
			scheduler.shutdown();
			future.cancel(true);
		}
	}

}
