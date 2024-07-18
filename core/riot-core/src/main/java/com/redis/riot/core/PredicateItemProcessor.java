package com.redis.riot.core;

import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;

/**
 * Function that only keeps items that match the given predicate., i.e. a given
 * item is kept only if predicate.test(item) == true.
 * 
 * @param <T>
 */
public class PredicateItemProcessor<T> implements ItemProcessor<T, T> {

	private final Predicate<T> predicate;

	public PredicateItemProcessor(Predicate<T> predicate) {
		this.predicate = predicate;
	}

	@Override
	public T process(T item) {
		if (predicate.test(item)) {
			return item;
		}
		return null;
	}

}
