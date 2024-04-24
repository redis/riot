package com.redis.riot.core.function;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public class CompositeOperator<T> implements UnaryOperator<T> {

	private final List<Consumer<T>> consumers;

	@SuppressWarnings("unchecked")
	public CompositeOperator(Consumer<T>... consumers) {
		this(Arrays.asList(consumers));
	}

	public CompositeOperator(List<Consumer<T>> consumers) {
		this.consumers = consumers;
	}

	@Override
	public T apply(T t) {
		consumers.forEach(c -> c.accept(t));
		return t;
	}

}
