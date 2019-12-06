package com.redislabs.riot.batch;

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
public class StepThread<I, O> implements Runnable {

	private ItemReader<I> reader;
	private ItemWriter<O> writer;
	private ChunkedIterator<I, O> iterator;
	@Getter
	private long readCount;
	@Getter
	private long writeCount;
	@Getter
	private boolean running;
	private int id;

	public StepThread(int threadId, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer,
			int chunkSize) {
		this.id = threadId;
		this.reader = reader;
		this.writer = writer;
		this.iterator = processor == null ? new ChunkedIterator<>(reader, chunkSize)
				: new ProcessingChunkedIterator<>(reader, chunkSize, processor);
	}

	public void open(ExecutionContext executionContext) {
		if (reader instanceof ItemStream) {
			((ItemStream) reader).open(executionContext);
		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).open(executionContext);
		}
	}

	@Override
	public void run() {
		this.running = true;
		while (iterator.hasNext()) {
			List<O> items = iterator.next();
			readCount += items.size();
			try {
				writer.write(items);
				writeCount += items.size();
			} catch (Exception e) {
				log.error("Could not write items", e);
			}
		}
		log.debug("StepThread #{} finished", id);
		this.running = false;
	}

	public void close() {
		if (writer instanceof ItemStream) {
			((ItemStream) writer).close();
		}
		if (reader instanceof ItemStream) {
			((ItemStream) reader).close();
		}
	}

}
