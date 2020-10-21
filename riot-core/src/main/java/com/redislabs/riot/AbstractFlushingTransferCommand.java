package com.redislabs.riot;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import picocli.CommandLine.Option;

public abstract class AbstractFlushingTransferCommand<I, O> extends AbstractTransferCommand<I, O> {

	@Option(names = "--flush-interval", description = "Duration between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long flushPeriod = 50;

	protected Long flushPeriod() {
		return flushPeriod;
	}

	@Override
	public CompletableFuture<Void> execute(Transfer<I, O> transfer) {
		CompletableFuture<Void> future = super.execute(transfer);
		Long flushPeriod = flushPeriod();
		if (flushPeriod != null) {
			ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
			ScheduledFuture<?> flushFuture = scheduler.scheduleAtFixedRate(transfer::flush, flushPeriod, flushPeriod,
					TimeUnit.MILLISECONDS);
			future.whenComplete((r, t) -> {
				scheduler.shutdown();
				flushFuture.cancel(true);
			});
		}
		return future;
	}

}
