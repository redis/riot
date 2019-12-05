package com.redislabs.riot.batch;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StepExecutor<I, O> implements Runnable {

	private ItemReader<I> reader;
	private ItemProcessor<I, O> processor;
	private ItemWriter<O> writer;
	private int chunkSize;

	public StepExecutor(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer, int chunkSize) {
		this.reader = reader;
		this.processor = processor;
		this.writer = writer;
		this.chunkSize = chunkSize;
	}

	@Override
	public void run() {
		ExecutionContext executionContext = new ExecutionContext();
		if (reader instanceof ItemStream) {
			((ItemStream) reader).open(executionContext);
		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).open(executionContext);
		}
		ChunkedIterator<I, O> iterator = processor == null ? new ChunkedIterator<>(reader, chunkSize)
				: new ProcessingChunkedIterator<>(reader, chunkSize, processor);
		while (iterator.hasNext()) {
			try {
				writer.write(iterator.next());
			} catch (Exception e) {
				log.error("Could not write items", e);
			}
		}
		if (writer instanceof ItemStream) {
			((ItemStream) writer).close();
		}
		if (reader instanceof ItemStream) {
			((ItemStream) reader).close();
		}

	}

}
