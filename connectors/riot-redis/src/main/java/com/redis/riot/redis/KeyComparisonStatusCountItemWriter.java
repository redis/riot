package com.redis.riot.redis;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;

public class KeyComparisonStatusCountItemWriter extends AbstractItemStreamItemWriter<KeyComparison<?>> {

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

	public List<Long> getCounts(Status... statuses) {
		return Stream.of(statuses).map(this::getCount).collect(Collectors.toList());
	}

	public long getTotal() {
		return counts.values().stream().collect(Collectors.summingLong(AtomicLong::get));
	}

	@Override
	public void write(Chunk<? extends KeyComparison<?>> items) {
		for (KeyComparison<?> comparison : items) {
			incrementAndGet(comparison.getStatus());
		}
	}

}
