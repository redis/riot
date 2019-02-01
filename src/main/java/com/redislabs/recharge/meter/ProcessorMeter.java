package com.redislabs.recharge.meter;

import org.springframework.batch.core.ItemProcessListener;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Component
public class ProcessorMeter<S, T> implements ItemProcessListener<S, T> {

	private Counter processed;

	public ProcessorMeter(MeterRegistry registry) {
		processed = registry.counter("processor.items");
	}

	@Override
	public void beforeProcess(S item) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterProcess(S item, T result) {
		processed.increment();
	}

	@Override
	public void onProcessError(S item, Exception e) {
		// TODO Auto-generated method stub

	}

}
