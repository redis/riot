package com.redis.riot.core;

import java.util.List;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.springframework.beans.factory.InitializingBean;

import com.hrakaroo.glob.GlobPattern;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.RedisCodec;

public class KeyFilter<K> implements Predicate<K>, InitializingBean {

	private final RedisCodec<K, ?> codec;
	private final Function<K, String> toString;
	private KeyFilterOptions options = new KeyFilterOptions();
	private Predicate<K> predicate;

	public KeyFilter(RedisCodec<K, ?> codec) {
		this.codec = codec;
		this.toString = BatchUtils.toStringKeyFunction(codec);
	}

	@Override
	public void afterPropertiesSet() {
		this.predicate = predicate();
	}

	@Override
	public boolean test(K t) {
		return predicate.test(t);
	}

	public Predicate<K> predicate() {
		if (options.isEmptyIncludes() && options.isEmptyExcludes()) {
			return slotsPredicate();
		}
		Predicate<String> stringGlobPredicate = stringGlobPredicate();
		Predicate<K> globPredicate = k -> stringGlobPredicate.test(toString.apply(k));
		if (options.isEmptySlots()) {
			return globPredicate;
		}
		return slotsPredicate().and(globPredicate);
	}

	private Predicate<K> slotsPredicate() {
		return options.getSlots().stream().map(r -> slotRangePredicate(r.getStart(), r.getEnd())).reduce(k -> false,
				Predicate::or);
	}

	private Predicate<String> stringGlobPredicate() {
		if (options.isEmptyIncludes()) {
			return excludesPredicate();
		}
		if (options.isEmptyExcludes()) {
			return includesPredicate();
		}
		return includesPredicate().and(excludesPredicate());
	}

	private Predicate<String> includesPredicate() {
		return globPredicate(options.getIncludes());
	}

	private Predicate<String> excludesPredicate() {
		return globPredicate(options.getExcludes()).negate();
	}

	private Predicate<String> globPredicate(List<String> patterns) {
		return patterns.stream().map(this::globPredicate).reduce(k -> false, Predicate::or);
	}

	private Predicate<String> globPredicate(String glob) {
		return GlobPattern.compile(glob)::matches;
	}

	public static IntPredicate between(int start, int end) {
		return i -> i >= start && i <= end;
	}

	private int slot(K key) {
		return SlotHash.getSlot(codec.encodeKey(key));
	}

	public Predicate<K> slotRangePredicate(int start, int end) {
		IntPredicate rangePredicate = between(start, end);
		return k -> rangePredicate.test(slot(k));
	}

	public KeyFilterOptions getOptions() {
		return options;
	}

	public void setOptions(KeyFilterOptions options) {
		this.options = options;
	}

}
