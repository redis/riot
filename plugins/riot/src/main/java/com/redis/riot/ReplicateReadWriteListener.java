package com.redis.riot;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.observability.BatchMetrics;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.KeyValue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class ReplicateReadWriteListener<K> implements ItemWriteListener<KeyValue<K>>, ItemReadListener<KeyValue<K>> {

	public static final String METRICS_PREFIX = "riot.replicate.";

	private static final String METRIC_READ = "read";
	private static final String METRIC_WRITE = "write";

	private MeterRegistry meterRegistry = Metrics.globalRegistry;

	@Override
	public void afterWrite(Chunk<? extends KeyValue<K>> items) {
		onItems(items, METRIC_WRITE, BatchMetrics.STATUS_SUCCESS);
	}

	@Override
	public void onWriteError(Exception exception, Chunk<? extends KeyValue<K>> items) {
		onItems(items, METRIC_WRITE, BatchMetrics.STATUS_FAILURE);
	}

	@Override
	public void afterRead(KeyValue<K> item) {
		onItem(item, METRIC_READ, BatchMetrics.STATUS_SUCCESS);
	}

	private void onItems(Chunk<? extends KeyValue<K>> items, String metric, String status) {
		for (KeyValue<K> item : items) {
			onItem(item, metric, status);
		}
	}

	private void onItem(KeyValue<K> item, String metric, String status) {
		Tags tags = Tags.of("event", item.getEvent(), "status", status);
		if (item.getType() != null) {
			tags = tags.and("type", item.getType());
		}
		Timer timer = meterRegistry.timer(METRICS_PREFIX + metric, tags);
		timer.record(System.currentTimeMillis() - item.getTimestamp(), TimeUnit.MILLISECONDS);
	}

	public void setMeterRegistry(MeterRegistry registry) {
		this.meterRegistry = registry;
	}

}
