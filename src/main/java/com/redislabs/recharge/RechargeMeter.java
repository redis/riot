package com.redislabs.recharge;

import java.util.List;

import org.springframework.batch.core.listener.ItemListenerSupport;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Component
public class RechargeMeter<S, T> extends ItemListenerSupport<S, T> {

	private Counter reads;
	private Counter processed;
	private Counter writes;

	public RechargeMeter(MeterRegistry registry) {
		reads = registry.counter("reader.items");
		processed = registry.counter("processor.items");
		writes = registry.counter("writer.items");
	}

	@Override
	public void afterRead(S item) {
		reads.increment();
	}

	@Override
	public void afterProcess(S item, T result) {
		processed.increment();
	}

	@Override
	public void afterWrite(List<? extends T> items) {
		writes.increment(items.size());
	}

}
