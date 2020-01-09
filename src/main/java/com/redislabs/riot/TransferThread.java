package com.redislabs.riot;

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TransferThread<I, O> implements Runnable {

	public final static String CONTEXT_PARTITION = "partition";
	public final static String CONTEXT_PARTITIONS = "partitions";

	private final int threadId;
	private final ItemReader<I> reader;
	private final ItemProcessor<I, O> processor;
	private final ItemWriter<O> writer;
	private final Transfer<I, O> transfer;
	@Getter
	private long readCount;
	@Getter
	private long writeCount;
	@Getter
	private boolean running;
	private ChunkedIterator<I, O> iterator;

	@Override
	public void run() {
		ExecutionContext executionContext = new ExecutionContext();
		executionContext.putInt(CONTEXT_PARTITION, threadId);
		executionContext.putInt(CONTEXT_PARTITIONS, transfer.getNThreads());
		if (reader instanceof ItemStream) {
			((ItemStream) reader).open(executionContext);
		}
		this.iterator = processor == null ? new ChunkedIterator<>(reader, transfer.getBatchSize())
				: new ProcessingChunkedIterator<>(reader, transfer.getBatchSize(), processor);
		if (writer instanceof ItemStream) {
			((ItemStream) writer).open(executionContext);
		}
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
		this.running = false;
	}

}
