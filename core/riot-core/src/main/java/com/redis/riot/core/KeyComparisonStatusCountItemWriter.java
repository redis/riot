package com.redis.riot.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparison.Status;

public class KeyComparisonStatusCountItemWriter extends AbstractItemStreamItemWriter<KeyComparison> {

    private final Map<Status, AtomicLong> counts = Stream.of(Status.values())
            .collect(Collectors.toMap(s -> s, s -> new AtomicLong()));

    private long incrementAndGet(Status status) {
        return counts.get(status).incrementAndGet();
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

    public Long[] getCounts(Status... statuses) {
        Long[] array = new Long[statuses.length];
        for (int index = 0; index < statuses.length; index++) {
            array[index] = getCount(statuses[index]);
        }
        return array;
    }

    public long getTotal() {
        return counts.values().stream().collect(Collectors.summingLong(AtomicLong::get));
    }

    @Override
    public void write(List<? extends KeyComparison> items) {
        for (KeyComparison comparison : items) {
            incrementAndGet(comparison.getStatus());
        }
    }

}
