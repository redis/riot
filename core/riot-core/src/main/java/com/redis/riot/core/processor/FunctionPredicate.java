package com.redis.riot.core.processor;

import java.util.function.Function;
import java.util.function.Predicate;

public class FunctionPredicate<S, T> implements Predicate<S> {

	private final Function<S, T> function;
	private final Predicate<T> predicate;

	public FunctionPredicate(Function<S, T> function, Predicate<T> predicate) {
		this.function = function;
		this.predicate = predicate;
	}

	@Override
	public boolean test(S t) {
		return predicate.test(function.apply(t));
	}

}
