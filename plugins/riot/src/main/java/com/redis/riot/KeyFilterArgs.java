package com.redis.riot;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.Range;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.RedisCodec;
import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class KeyFilterArgs {

	@Option(names = "--key-include", arity = "1..*", description = "Glob pattern to match keys for inclusion. E.g. 'mykey:*' will only consider keys starting with 'mykey:'.", paramLabel = "<exp>")
	private List<String> includes;

	@Option(names = "--key-exclude", arity = "1..*", description = "Glob pattern to match keys for exclusion. E.g. 'mykey:*' will exclude keys starting with 'mykey:'.", paramLabel = "<exp>")
	private List<String> excludes;

	@Option(names = "--key-slot", arity = "1..*", description = "Ranges of key slots to consider for processing. For example '0-8000' will only consider keys that fall within the range 0 to 8000.", paramLabel = "<range>")
	private List<Range> slots;

	public <K> Optional<Predicate<K>> predicate(RedisCodec<K, ?> codec) {
		Optional<Predicate<K>> slotsPredicate = slotsPredicate(codec);
		Optional<Predicate<K>> globPredicate = globPredicate(codec);
		if (slotsPredicate.isPresent()) {
			if (globPredicate.isPresent()) {
				return Optional.of(slotsPredicate.get().and(globPredicate.get()));
			}
			return slotsPredicate;
		}
		return globPredicate;
	}

	private <K> Optional<Predicate<K>> globPredicate(RedisCodec<K, ?> codec) {
		Optional<Predicate<String>> stringPredicate = globPredicate();
		Function<K, String> toString = BatchUtils.toStringKeyFunction(codec);
		return stringPredicate.map(p -> encodePredicate(p, toString));
	}

	private <K> Predicate<K> encodePredicate(Predicate<String> predicate, Function<K, String> toString) {
		return k -> predicate.test(toString.apply(k));
	}

	private Optional<Predicate<String>> globPredicate() {
		Optional<Predicate<String>> includePredicate = globPredicate(includes);
		Optional<Predicate<String>> excludePredicate = globPredicate(excludes).map(Predicate::negate);
		if (includePredicate.isPresent()) {
			if (excludePredicate.isPresent()) {
				return Optional.of(includePredicate.get().and(excludePredicate.get()));
			}
			return includePredicate;
		}
		return excludePredicate;
	}

	private Optional<Predicate<String>> globPredicate(List<String> patterns) {
		if (CollectionUtils.isEmpty(patterns)) {
			return Optional.empty();
		}
		return Optional.of(patterns.stream().map(BatchUtils::globPredicate).reduce(k -> false, Predicate::or));
	}

	private <K> Optional<Predicate<K>> slotsPredicate(RedisCodec<K, ?> codec) {
		if (CollectionUtils.isEmpty(slots)) {
			return Optional.empty();
		}
		return Optional.of(slots.stream().map(r -> slotRangePredicate(codec, r)).reduce(k -> false, Predicate::or));
	}

	public static IntPredicate between(int start, int end) {
		return i -> i >= start && i <= end;
	}

	private <K> int slot(RedisCodec<K, ?> codec, K key) {
		return SlotHash.getSlot(codec.encodeKey(key));
	}

	public <K> Predicate<K> slotRangePredicate(RedisCodec<K, ?> codec, Range range) {
		IntPredicate rangePredicate = between(range.getMin(), range.getMax());
		return k -> rangePredicate.test(slot(codec, k));
	}

	public List<String> getIncludes() {
		return includes;
	}

	public void setIncludes(List<String> includes) {
		this.includes = includes;
	}

	public List<String> getExcludes() {
		return excludes;
	}

	public void setExcludes(List<String> excludes) {
		this.excludes = excludes;
	}

	public List<Range> getSlots() {
		return slots;
	}

	public void setSlots(List<Range> slots) {
		this.slots = slots;
	}

}
