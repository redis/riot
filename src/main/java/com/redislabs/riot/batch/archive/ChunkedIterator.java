package com.redislabs.riot.batch.archive;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.item.ItemReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChunkedIterator<I, O> implements Iterator<List<O>> {

	private ItemReader<I> reader;
	private int chunkSize;
	private boolean finished;
	private List<O> items;

	public ChunkedIterator(ItemReader<I> reader, int chunkSize) {
		this.reader = reader;
		this.chunkSize = chunkSize;
		this.items = new ArrayList<>(chunkSize);
	}

	@Override
	public boolean hasNext() {
		if (finished) {
			return false;
		}
		items.clear();
		while (items.size() < chunkSize && !finished) {
			I item;
			try {
				item = reader.read();
			} catch (Exception e) {
				log.error("Could not get next item from reader", e);
				continue;
			}
			if (item == null) {
				finished = true;
			} else {
				items.add(process(item));
			}
		}
		return !items.isEmpty();
	}

	@SuppressWarnings("unchecked")
	protected O process(I item) {
		return (O) item;
	}

	@Override
	public List<O> next() {
		return items;
	}

}
