package com.redis.riot.core;

import java.util.function.Consumer;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

public class LoggingWriteListener<T> implements ItemWriteListener<T> {

	private final Consumer<Chunk<? extends T>> consumer;

	public LoggingWriteListener(Consumer<Chunk<? extends T>> log) {
		this.consumer = log;
	}

	@Override
	public void afterWrite(Chunk<? extends T> items) {
		consumer.accept(items);
	}

}
