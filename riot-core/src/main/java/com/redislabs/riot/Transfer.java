package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.BatchTransfer;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.ProgressReporter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.util.Assert;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Transfer<I, O> implements ProgressReporter {

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

    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> executeAsync() {
	CompletableFuture<Void>[] futures = new CompletableFuture[threads.size()];
	for (int index = 0; index < threads.size(); index++) {
	    futures[index] = CompletableFuture.runAsync(threads.get(index));
	}
	return CompletableFuture.allOf(futures);
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
	if (reader instanceof KeyValueItemReader) {
	    ((KeyValueItemReader<?, ?, ?>) reader).flush();
	}
	for (BatchTransfer<I> thread : threads) {
	    try {
		thread.flush();
	    } catch (Exception e) {
		log.error("Could not flush", e);
	    }
	}
    }

    @Override
    public Long getTotal() {
	if (reader instanceof ProgressReporter) {
	    return ((ProgressReporter) reader).getTotal();
	}
	return null;
    }

    @Override
    public long getDone() {
	if (reader instanceof ProgressReporter) {
	    return ((ProgressReporter) reader).getDone();
	}
	return threads.stream().mapToLong(BatchTransfer::getCount).sum();
    }

}
