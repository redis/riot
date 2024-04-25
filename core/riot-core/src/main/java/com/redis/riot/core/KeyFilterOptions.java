package com.redis.riot.core;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.codec.RedisCodec;

public class KeyFilterOptions {

	private List<String> includes;
	private List<String> excludes;
	private List<SlotRange> slots;

	public List<String> getIncludes() {
		return includes;
	}

	public void setIncludes(List<String> patterns) {
		this.includes = patterns;
	}

	public List<String> getExcludes() {
		return excludes;
	}

	public void setExcludes(List<String> patterns) {
		this.excludes = patterns;
	}

	public List<SlotRange> getSlots() {
		return slots;
	}

	public void setSlots(List<SlotRange> ranges) {
		this.slots = ranges;
	}

	public boolean isEmpty() {
		return CollectionUtils.isEmpty(includes) && CollectionUtils.isEmpty(excludes) && CollectionUtils.isEmpty(slots);
	}

	public <K> Predicate<K> predicate(RedisCodec<K, ?> codec) {
		return slotsPredicate(codec).and(globPredicate(codec));
	}

	private <K> Predicate<K> slotsPredicate(RedisCodec<K, ?> codec) {
		if (CollectionUtils.isEmpty(slots)) {
			return Predicates.isTrue();
		}
		Stream<Predicate<K>> predicates = slots.stream()
				.map(r -> Predicates.slotRange(codec, r.getStart(), r.getEnd()));
		return Predicates.or(predicates);
	}

	private <K> Predicate<K> globPredicate(RedisCodec<K, ?> codec) {
		return Predicates.map(BatchUtils.toStringKeyFunction(codec), globPredicate());
	}

	private Predicate<String> globPredicate() {
		Predicate<String> include = RiotUtils.globPredicate(includes);
		if (CollectionUtils.isEmpty(excludes)) {
			return include;
		}
		return include.and(RiotUtils.globPredicate(excludes).negate());
	}

	public <K> ItemProcessor<K, K> processor(RedisCodec<K, ?> codec) {
		if (isEmpty()) {
			return null;
		}
		return new PredicateItemProcessor<>(predicate(codec));
	}

}
