package com.redislabs.riot.batch.processor;

public class SpelProcessorContext {

	private SpelProcessor processor;

	public SpelProcessorContext(SpelProcessor processor) {
		this.processor = processor;
	}

	public long index() {
		return processor.getIndex();
	}

}
