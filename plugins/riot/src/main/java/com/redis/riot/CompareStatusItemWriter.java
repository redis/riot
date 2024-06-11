package com.redis.riot;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;

public class CompareStatusItemWriter<K> extends AbstractItemStreamItemWriter<KeyComparison<K>> {

	public static final Predicate<StatusCount> MISMATCHES = s -> s.getStatus() != Status.OK && s.getCount() > 0;

	public static class StatusCount {

		private final Status status;
		private final long count;

		public StatusCount(Status status, long count) {
			this.status = status;
			this.count = count;
		}

		public Status getStatus() {
			return status;
		}

		public long getCount() {
			return count;
		}

	}

	private final Map<Status, AtomicLong> counts = Stream.of(Status.values())
			.collect(Collectors.toMap(Function.identity(), s -> new AtomicLong()));

	private long incrementAndGet(Status status) {
		return counts.get(status).incrementAndGet();
	}

	public long getOK() {
		return getCount(Status.OK);
	}

	public long getMissing() {
		return getCount(Status.MISSING);
	}

	public long getType() {
		return getCount(Status.TYPE);
	}

	public long getTtl() {
		return getCount(Status.TTL);
	}

	public long getValue() {
		return getCount(Status.VALUE);
	}

	public long getCount(Status status) {
		return counts.get(status).get();
	}

	public List<StatusCount> getMismatches() {
		return getCounts(MISMATCHES);
	}

	public List<StatusCount> getCounts(Predicate<? super StatusCount> predicate) {
		return counts.entrySet().stream().map(e -> new StatusCount(e.getKey(), e.getValue().get())).filter(predicate)
				.collect(Collectors.toList());
	}

	public long getTotal() {
		return counts.values().stream().collect(Collectors.summingLong(AtomicLong::get));
	}

	@Override
	public void write(Chunk<? extends KeyComparison<K>> items) {
		for (KeyComparison<?> comparison : items) {
			incrementAndGet(comparison.getStatus());
		}
	}

}
