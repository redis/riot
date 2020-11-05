package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

public class ProcessingItemWriter<I, O> extends AbstractItemStreamItemWriter<I> {

    private final ItemProcessor<I, O> processor;
    private final ItemWriter<O> writer;

    public ProcessingItemWriter(ItemProcessor<I, O> processor, ItemWriter<O> writer) {
	setName(ClassUtils.getShortName(getClass()));
	this.processor = processor;
	this.writer = writer;
    }

    @Override
    public void write(List<? extends I> items) throws Exception {
	List<O> targetItems = new ArrayList<>(items.size());
	for (I item : items) {
	    targetItems.add(processor.process(item));
	}
	writer.write(targetItems);
    }
}
