package com.redis.riot;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

public class ToMapFunction<T, K, V> implements Function<T, Map<K, V>> {

	private final List<? extends Function<T, Map<K, V>>> functions;

	@SuppressWarnings("unchecked")
	public ToMapFunction(Function<T, Map<K, V>>... functions) {
		this(Arrays.asList(functions));

	}

	public ToMapFunction(List<? extends Function<T, Map<K, V>>> functions) {
		Assert.notEmpty(functions, "At least one function must be given");
		this.functions = functions;
	}

	@Override
	public Map<K, V> apply(T t) {
		Iterator<? extends Function<T, Map<K, V>>> iterator = functions.iterator();
		Map<K, V> map = iterator.next().apply(t);
		while (iterator.hasNext()) {
			Map<K, ? extends V> values = iterator.next().apply(t);
			if (!CollectionUtils.isEmpty(values)) {
				map.putAll(values);
			}
		}
		return map;
	}

}