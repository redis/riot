package com.redislabs.riot.batch;

public class SpelProcessorContext {

	private SpelProcessor processor;

	public SpelProcessorContext(SpelProcessor processor) {
		this.processor = processor;
	}

	public long index() {
		return processor.getIndex();
	}

}
