package com.redislabs.riot;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import picocli.CommandLine.Option;

public abstract class AbstractFlushingTransferCommand<I, O> extends AbstractTransferCommand<I, O> {

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<Transfer<I, O>, ScheduledFuture<?>> futures = new HashMap<>();

    @Option(names = "--flush-interval", description = "Duration between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long flushPeriod = 50;

    protected abstract boolean flushingEnabled();

    @Override
    public void open(Transfer<I, O> transfer) {
	super.open(transfer);
	if (flushingEnabled()) {
	    futures.put(transfer,
		    scheduler.scheduleAtFixedRate(transfer::flush, flushPeriod, flushPeriod, TimeUnit.MILLISECONDS));
	}
    }

    @Override
    public void close(Transfer<I, O> transfer) {
	if (futures.containsKey(transfer)) {
	    futures.get(transfer).cancel(true);
	}
	super.close(transfer);
    }

}
