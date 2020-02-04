package com.redislabs.riot.transfer;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings({ "rawtypes", "unchecked" })
public class FlowThread implements Runnable {

	public final static String CONTEXT_PARTITION = "partition";
	public final static String CONTEXT_PARTITIONS = "partitions";

	private @Setter int threadId;
	private @Setter int threads;
	private @Setter Flow flow;
	private @Setter Batcher batcher;
	private @Setter Long flushRate;
	private @Getter long readCount;
	private @Getter long writeCount;
	private @Getter boolean running;
	private boolean stopped;

	@Override
	public void run() {
		try {
			ExecutionContext executionContext = new ExecutionContext();
			executionContext.putInt(CONTEXT_PARTITION, threadId);
			executionContext.putInt(CONTEXT_PARTITIONS, threads);
			if (flow.reader() instanceof ItemStream) {
				((ItemStream) flow.reader()).open(executionContext);
			}
			if (flow.writer() instanceof ItemStream) {
				((ItemStream) flow.writer()).open(executionContext);
			}
			ScheduledExecutorService scheduler = null;
			ScheduledFuture<?> flushFuture = null;
			if (flushRate != null) {
				scheduler = Executors.newSingleThreadScheduledExecutor();
				flushFuture = scheduler.scheduleAtFixedRate(this::flush, flushRate, flushRate, TimeUnit.MILLISECONDS);
			}
			this.running = true;
			List items;
			while ((items = batcher.next()) != null && !stopped) {
				write(items);
			}
			if (scheduler != null) {
				scheduler.shutdown();
			}
			if (flushFuture != null) {
				flushFuture.cancel(true);
			}
			log.debug("Closing reader");
			if (flow.reader() instanceof ItemStream) {
				((ItemStream) flow.reader()).close();
			}
			if (flow.writer() instanceof ItemStream) {
				((ItemStream) flow.writer()).close();
			}
			this.running = false;
			log.debug("Flow {} thread {} finished", flow.name(), threadId);
		} catch (Throwable e) {
			log.error("Flow {} execution failed", flow.name(), e);
		}
	}

	public Metrics progress() {
		return new Metrics().reads(readCount).writes(writeCount).runningThreads(running ? 1 : 0);
	}

	public void stop() {
		stopped = true;
	}

	private void write(List items) {
		readCount += items.size();
		try {
			flow.writer().write(items);
			writeCount += items.size();
		} catch (Exception e) {
			log.error("Could not write items", e);
		}
	}

	private void flush() {
		List items = batcher.flush();
		if (!items.isEmpty()) {
			write(items);
		}
	}

}
