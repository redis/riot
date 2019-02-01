package com.redislabs.recharge.meter;

import org.springframework.batch.core.ItemReadListener;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Component
public class ReaderMeter<T> implements ItemReadListener<T> {

	private Counter reads;

	public ReaderMeter(MeterRegistry registry) {
		reads = registry.counter("reader.items");
	}

	@Override
	public void beforeRead() {
	}

	@Override
	public void afterRead(T item) {
		reads.increment();
	}

	@Override
	public void onReadError(Exception ex) {

	}

}
