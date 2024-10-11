package com.redis.riot;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.item.redis.common.KeyValue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class ReplicateLagWriteListener implements ItemWriteListener<KeyValue<byte[]>> {

	private static final String METRIC_NAME = "riot.replicate.lag";

	private MeterRegistry meterRegistry = Metrics.globalRegistry;

	@Override
	public void afterWrite(Chunk<? extends KeyValue<byte[]>> items) {
		items.forEach(this::afterWrite);
	}

	private void afterWrite(KeyValue<byte[]> item) {
		long lagMillis = System.currentTimeMillis() - item.getTimestamp();
		Timer timer = meterRegistry.timer(METRIC_NAME, Tags.of("event", item.getEvent(), "type", item.getType()));
		timer.record(lagMillis, TimeUnit.MILLISECONDS);
	}

	public void setMeterRegistry(MeterRegistry registry) {
		this.meterRegistry = registry;
	}

}
