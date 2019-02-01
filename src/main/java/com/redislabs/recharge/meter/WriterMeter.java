package com.redislabs.recharge.meter;

import java.util.List;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Component
public class WriterMeter<T> implements ItemWriteListener<T> {

	private Counter writes;

	public WriterMeter(MeterRegistry registry) {
		writes = registry.counter("writer.items");
	}

	@Override
	public void beforeWrite(List<? extends T> items) {
	}

	@Override
	public void afterWrite(List<? extends T> items) {
		writes.increment(items.size());
	}

	@Override
	public void onWriteError(Exception exception, List<? extends T> items) {
	}

}
