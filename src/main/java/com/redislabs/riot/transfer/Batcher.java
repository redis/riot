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
@SuppressWarnings({ "rawtypes", "unchecked" })
public class Batcher {

	private ItemReader reader;
	private int chunkSize;
	private boolean finished;
	private ItemProcessor processor;
	private BlockingQueue items;

	@Builder
	private Batcher(ItemReader reader, ItemProcessor processor, int chunkSize) {
		this.reader = reader;
		this.processor = processor;
		this.chunkSize = chunkSize;
		this.items = new LinkedBlockingDeque(1000);
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
				log.error("Could not get next item from reader", e);
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
