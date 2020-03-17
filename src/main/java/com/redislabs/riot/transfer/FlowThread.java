package com.redislabs.riot.transfer;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings({ "rawtypes", "unchecked" })
public class FlowThread<I, O> implements Runnable {

	public final static String CONTEXT_PARTITION = "partition";
	public final static String CONTEXT_PARTITIONS = "partitions";

	private int threadId;
	private int threads;
	private Flow<I, O> flow;
	private Batcher<I, O> batcher;
	private Long flushRate;
	private long readCount;
	private long writeCount;
	private boolean running;
	private boolean stopped;

	@Builder
	public FlowThread(int threadId, int threads, Flow<I, O> flow, Batcher<I, O> batcher, Long flushRate) {
		super();
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
			if (flow.getReader() instanceof ItemStream) {
				((ItemStream) flow.getReader()).open(executionContext);
			}
			if (flow.getWriter() instanceof ItemStream) {
				((ItemStream) flow.getWriter()).open(executionContext);
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
			if (flow.getReader() instanceof ItemStream) {
				((ItemStream) flow.getReader()).close();
			}
			if (flow.getWriter() instanceof ItemStream) {
				((ItemStream) flow.getWriter()).close();
			}
			this.running = false;
			log.debug("Flow {} thread {} finished", flow.getName(), threadId);
		} catch (Throwable e) {
			log.error("Flow {} execution failed", flow.getName(), e);
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
			flow.getWriter().write(items);
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
