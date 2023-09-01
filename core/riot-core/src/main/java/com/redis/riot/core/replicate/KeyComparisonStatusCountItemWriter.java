package com.redis.riot.core.replicate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.util.KeyComparison;
import com.redis.spring.batch.util.KeyComparison.Status;

public class KeyComparisonStatusCountItemWriter extends AbstractItemStreamItemWriter<KeyComparison> {

    private final Map<Status, AtomicLong> counts = Stream.of(Status.values())
            .collect(Collectors.toMap(s -> s, s -> new AtomicLong()));

    private long incrementAndGet(Status status) {
        return counts.get(status).incrementAndGet();
    }

    public long getCount(Status status) {
        return counts.get(status).get();
    }

    public long getTotalCount() {
        return counts.values().stream().collect(Collectors.summingLong(AtomicLong::get));
    }

    @Override
    public void write(List<? extends KeyComparison> items) {
        for (KeyComparison comparison : items) {
            incrementAndGet(comparison.getStatus());
        }
    }

}
