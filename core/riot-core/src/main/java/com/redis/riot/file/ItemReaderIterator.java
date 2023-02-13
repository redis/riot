package com.redis.riot.file;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;

public class ItemReaderIterator<T> implements Iterator<T> {

	private final ItemReader<T> reader;
	private T current;

	public ItemReaderIterator(ItemReader<T> reader) throws Exception {
		this.reader = reader;
		if (reader instanceof ItemStream) {
			((ItemStream) reader).open(new ExecutionContext());
		}
		current = reader.read();
	}

	@Override
	public boolean hasNext() {
		return current != null;
	}

	@Override
	public T next() {
		if (current == null) {
			throw new NoSuchElementException();
		}
		T result = current;
		try {
			current = reader.read();
		} catch (Exception e) {
			throw new IllegalStateException("Could not read next item from ItemReader", e);
		}
		return result;
	}

}
