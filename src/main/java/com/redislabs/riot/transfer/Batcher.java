package com.redislabs.riot.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings({ "rawtypes", "unchecked" })
public class Batcher {

	private @Setter ItemReader reader;
	private @Setter int chunkSize;
	private @Setter boolean finished;
	private @Setter ItemProcessor processor;
	private @Setter ErrorHandler errorHandler;
	private BlockingQueue items;

	public Batcher queueCapacity(int capacity) {
		this.items = new LinkedBlockingDeque(capacity);
		return this;
	}

	public List next() {
		if (finished) {
			return null;
		}
		while (items.size() < chunkSize && !finished) {
			Object item;
			try {
				item = reader.read();
			} catch (Exception e) {
				errorHandler.handle(e);
				continue;
			}
			if (item == null) {
				log.debug("Batcher finished");
				finished = true;
			} else {
				Object processedItem;
				try {
					processedItem = processor.process(item);
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
		List result = new ArrayList<>(chunkSize);
		items.drainTo(result, chunkSize);
		return result;
	}

	public List flush() {
		List result = new ArrayList<>(items.size());
		items.drainTo(result);
		return result;
	}

}
