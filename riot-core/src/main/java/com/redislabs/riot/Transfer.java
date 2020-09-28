package com.redislabs.riot;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.*;
import org.springframework.batch.item.redis.support.BatchRunnable;
import org.springframework.batch.item.redis.support.RedisItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class Transfer<I, O> {

	@Getter
	private final String name;
	@Getter
	private final ItemReader<I> reader;
	@Getter
	private final ItemProcessor<I, O> processor;
	@Getter
	private final ItemWriter<O> writer;
	@Setter
	private int threadCount;
	@Setter
	private int batchSize;
	@Setter
	private Long flushPeriod;
	@Getter
	@Setter
	private Integer maxItemCount;
	private ArrayList<BatchRunnable<I>> threads;
	private ExecutorService executor;
	private ScheduledExecutorService scheduler;
	private ScheduledFuture<?> scheduledFuture = null;
	private List<TransferListener> listeners = new ArrayList<>();

	public Transfer(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		this.name = name;
		this.reader = reader;
		this.processor = processor;
		this.writer = writer;
	}

	public void addListener(TransferListener listener) {
		listeners.add(listener);
	}

	public static interface TransferListener {

		void onOpen();

		void onUpdate(long count);

		void onClose();

	}

	public void open() {
		Assert.isTrue(threadCount > 0, "Thread count must be greater than 0.");
		Assert.isTrue(batchSize > 0, "Batch size must be greater than 0.");
		this.threads = new ArrayList<>(threadCount);
		this.executor = Executors.newFixedThreadPool(threadCount);
		this.scheduler = Executors.newSingleThreadScheduledExecutor();
		ExecutionContext executionContext = new ExecutionContext();
		if (writer instanceof ItemStream) {
			log.debug("Opening writer");
			((ItemStream) writer).open(executionContext);
		}
		if (reader instanceof ItemStream) {
			if (maxItemCount != null) {
				if (reader instanceof AbstractItemCountingItemStreamItemReader) {
					log.debug("Configuring reader with maxItemCount={}", maxItemCount);
					((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(maxItemCount);
				}
			}
			log.debug("Opening reader");
			((ItemStream) reader).open(executionContext);
		}
		listeners.forEach(TransferListener::onOpen);
	}

	public void execute() {
		for (int index = 0; index < threadCount; index++) {
			threads.add(new BatchRunnable<>(reader(), new ProcessingItemWriter<>(processor, writer), batchSize));
		}
		threads.forEach(executor::submit);
		executor.shutdown();
		if (flushPeriod != null) {
			scheduledFuture = scheduler.scheduleAtFixedRate(this::flush, flushPeriod, flushPeriod,
					TimeUnit.MILLISECONDS);
		}
		while (!executor.isTerminated()) {
			try {
				executor.awaitTermination(100, TimeUnit.MILLISECONDS);
				listeners.forEach(l -> l.onUpdate(getWriteCount()));
			} catch (InterruptedException e) {
				log.debug("Interrupted while awaiting termination", e);
				throw new RuntimeException(e);
			}
		}
	}

	public void stop() {
		threads.forEach(BatchRunnable::stop);
	}

	public void close() {
		scheduler.shutdown();
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
		}
		if (reader instanceof ItemStream) {
			log.debug("Closing reader");
			((ItemStream) reader).close();
		}
		if (writer instanceof ItemStream) {
			log.debug("Closing writer");
			((ItemStream) writer).close();
		}
		listeners.forEach(TransferListener::onClose);
	}

	private ItemReader<I> reader() {
		if (threadCount > 1 && reader instanceof ItemStreamReader) {
			SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate((ItemStreamReader<I>) reader);
			return synchronizedReader;
		}
		return reader;
	}

	private void flush() {
		if (reader instanceof RedisItemReader) {
			((RedisItemReader<?, ?>) reader).flush();
		}
		for (BatchRunnable<I> thread : threads) {
			try {
				thread.flush();
			} catch (Exception e) {
				log.error("Could not flush", e);
			}
		}
	}

	private long getWriteCount() {
		return threads.stream().mapToLong(BatchRunnable::getWriteCount).sum();
	}

}
