package com.redis.riot.core.processor;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Unary operator that only keeps items that match the given predicate., i.e.
 * returns a given item if predicate.test(item) == true, null otherwise
 * 
 * @param <T>
 */
public class PredicateOperator<T> implements UnaryOperator<T> {

	private final Predicate<T> predicate;

	public PredicateOperator(Predicate<T> predicate) {
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
