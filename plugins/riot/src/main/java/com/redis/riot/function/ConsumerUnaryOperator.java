package com.redis.riot.function;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public class ConsumerUnaryOperator<T> implements UnaryOperator<T> {

	private final List<Consumer<T>> consumers;

	@SuppressWarnings("unchecked")
	public ConsumerUnaryOperator(Consumer<T>... consumers) {
		this(Arrays.asList(consumers));
	}

	public ConsumerUnaryOperator(List<Consumer<T>> consumers) {
		this.consumers = consumers;
	}

	@Override
	public T apply(T t) {
		for (Consumer<T> consumer : consumers) {
			consumer.accept(t);
		}
		return t;
	}
}
