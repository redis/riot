package com.redis.riot.core;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Function that only keeps items that match the given predicate., i.e. a given
 * item is kept only if predicate.test(item) == true.
 * 
 * @param <T>
 */
public class FilterFunction<T> implements UnaryOperator<T> {

	private final Predicate<T> predicate;

	public FilterFunction(Predicate<T> predicate) {
		this.predicate = predicate;
	}

	@Override
	public T apply(T item) {
		if (predicate.test(item)) {
			return item;
		}
		return null;
	}

}
