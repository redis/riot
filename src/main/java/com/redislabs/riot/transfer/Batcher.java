package com.redislabs.riot.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Batcher<I, O> {

	private ItemReader<I> reader;
	private int batchSize;
	private ItemProcessor<I, O> processor;
	private ErrorHandler errorHandler;
	private BlockingQueue<O> items;
	boolean finished = false;

	@Builder
	public Batcher(ItemReader<I> reader, int batchSize, ItemProcessor<I, O> processor, ErrorHandler errorHandler) {
		this.reader = reader;
		this.batchSize = batchSize;
		this.processor = processor;
		this.errorHandler = errorHandler;
		this.items = new LinkedBlockingDeque<>(batchSize);
	}

	@SuppressWarnings("unchecked")
	public List<O> next() {
		if (finished) {
			return null;
		}
		while (items.size() < batchSize && !finished) {
			I item;
			try {
				item = reader.read();
			} catch (Exception e) {
				errorHandler.handle(e);
				continue;
			}
			if (item == null) {
				finished = true;
			} else {
				O processedItem;
				try {
					processedItem = processor == null ? (O) item : processor.process(item);
				} catch (Exception e) {
					log.error("Could not process item", e);
					continue;
				}
				try {
					items.put(processedItem);
				} catch (InterruptedException e) {
					return null;
				}
			}
		}
		return flush();
	}

	public List<O> flush() {
		List<O> result = new ArrayList<>(items.size());
		items.drainTo(result);
		return result;
	}

}
