package com.redis.riot.core.processor;

import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public class ConsumerUnaryOperator<T> implements UnaryOperator<T> {

	private final Consumer<T> consumer;

	public ConsumerUnaryOperator(Consumer<T> consumer) {
		this.consumer = consumer;
	}

	@Override
	public T apply(T t) {
		consumer.accept(t);
		return t;
	}

}
