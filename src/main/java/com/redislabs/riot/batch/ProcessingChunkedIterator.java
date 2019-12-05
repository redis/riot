package com.redislabs.riot.batch;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessingChunkedIterator<I, O> extends ChunkedIterator<I, O> {

	private ItemProcessor<I, O> processor;

	public ProcessingChunkedIterator(ItemReader<I> reader, int chunkSize, ItemProcessor<I, O> processor) {
		super(reader, chunkSize);
		this.processor = processor;
	}

	@Override
	protected O process(I item) {
		try {
			return processor.process(item);
		} catch (Exception e) {
			log.error("Could not process item", e);
			return null;
		}
	}

}
