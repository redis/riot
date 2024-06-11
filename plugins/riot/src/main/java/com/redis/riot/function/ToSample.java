package com.redis.riot.function;

import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import com.redis.lettucemod.timeseries.Sample;

public class ToSample<T> implements Function<T, Sample> {

	private final ToLongFunction<T> timestampConverter;

	private final ToDoubleFunction<T> valueConverter;

	public ToSample(ToLongFunction<T> timestamp, ToDoubleFunction<T> value) {
		this.timestampConverter = timestamp;
		this.valueConverter = value;
	}

	@Override
	public Sample apply(T source) {
		double value = this.valueConverter.applyAsDouble(source);
		long timestamp = this.timestampConverter.applyAsLong(source);
		if (timestamp > 0) {
			return Sample.of(timestamp, value);
		}
		return Sample.of(value);
	}

}
