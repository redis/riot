package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.BatchTransfer;
import org.springframework.batch.item.redis.support.RedisItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.util.Assert;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Transfer<I, O> {

	@Getter
	private final ItemReader<I> reader;
	@Getter
	private final ItemWriter<? extends O> writer;
	private final List<BatchTransfer<I>> threads;

	@Builder
	public Transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer, int threads, int batch) {
		Assert.notNull(reader, "A reader instance is required.");
		Assert.notNull(writer, "A writer instance is required.");
		Assert.isTrue(threads > 0, "Thread count must be greater than 0.");
		Assert.isTrue(batch > 0, "Batch size must be greater than 0.");
		this.reader = reader;
		this.writer = writer;
		this.threads = new ArrayList<>(threads);
		for (int index = 0; index < threads; index++) {
			this.threads.add(new BatchTransfer<>(reader(), writer(processor, writer), batch));
		}
	}

	@SuppressWarnings("unchecked")
	private ItemWriter<I> writer(ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		if (processor == null) {
			return (ItemWriter<I>) writer;
		}
		return new ProcessingItemWriter<>(processor, writer);
	}

	@SuppressWarnings("rawtypes")
	public CompletableFuture<Void> execute() {
		ExecutionContext executionContext = new ExecutionContext();
		if (writer instanceof ItemStream) {
			log.debug("Opening writer");
			((ItemStream) writer).open(executionContext);
		}
		if (reader instanceof ItemStream) {
			log.debug("Opening reader");
			((ItemStream) reader).open(executionContext);
		}
		CompletableFuture[] futures = new CompletableFuture[threads.size()];
		for (int index = 0; index < threads.size(); index++) {
			futures[index] = CompletableFuture.runAsync(threads.get(index));
		}
		CompletableFuture<Void> future = CompletableFuture.allOf(futures);
		future.whenComplete((v, t) -> {
			if (reader instanceof ItemStream) {
				log.debug("Closing reader");
				((ItemStream) reader).close();
			}
			if (writer instanceof ItemStream) {
				log.debug("Closing writer");
				((ItemStream) writer).close();
			}
		});
		return future;
	}

	private ItemReader<I> reader() {
		if (threads.size() > 1 && reader instanceof ItemStreamReader) {
			SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate((ItemStreamReader<I>) reader);
			return synchronizedReader;
		}
		return reader;
	}

	public void flush() {
		if (reader instanceof RedisItemReader) {
			((RedisItemReader<?, ?>) reader).flush();
		}
		for (BatchTransfer<I> thread : threads) {
			try {
				thread.flush();
			} catch (Exception e) {
				log.error("Could not flush", e);
			}
		}
	}

	public long count() {
		return threads.stream().mapToLong(BatchTransfer::getCount).sum();
	}

}
