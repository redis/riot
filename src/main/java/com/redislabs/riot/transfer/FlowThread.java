package com.redislabs.riot.transfer;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class FlowThread implements Runnable {

	public final static String CONTEXT_PARTITION = "partition";
	public final static String CONTEXT_PARTITIONS = "partitions";

	private int threadId;
	private int threads;
	private Flow flow;
	private Batcher batcher;
	private Long flushRate;
	@Getter
	private long readCount;
	@Getter
	private long writeCount;
	@Getter
	private boolean running;
	private boolean stopped;

	@Builder
	private FlowThread(int threadId, int threads, Flow flow, Batcher batcher, Long flushRate) {
		this.threadId = threadId;
		this.threads = threads;
		this.flow = flow;
		this.batcher = batcher;
		this.flushRate = flushRate;
	}

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
		return Metrics.builder().reads(readCount).writes(writeCount).runningThreads(running ? 1 : 0).build();
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
