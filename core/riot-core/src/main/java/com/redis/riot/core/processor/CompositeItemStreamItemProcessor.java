package com.redis.riot.core.processor;

import java.util.Arrays;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.CompositeItemProcessor;

public class CompositeItemStreamItemProcessor<I, O> extends CompositeItemProcessor<I, O> implements ItemStream {

	private List<? extends ItemProcessor<?, ?>> delegates;

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		for (ItemProcessor<?, ?> delegate : delegates) {
			if (delegate instanceof ItemStream) {
				((ItemStream) delegate).open(executionContext);
			}
		}
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		for (ItemProcessor<?, ?> delegate : delegates) {
			if (delegate instanceof ItemStream) {
				((ItemStream) delegate).update(executionContext);
			}
		}
	}

	@Override
	public void close() throws ItemStreamException {
		for (ItemProcessor<?, ?> delegate : delegates) {
			if (delegate instanceof ItemStream) {
				((ItemStream) delegate).close();
			}
		}
	}

	@Override
	public void setDelegates(List<? extends ItemProcessor<?, ?>> delegates) {
		super.setDelegates(delegates);
		this.delegates = delegates;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <I, O> ItemProcessor<I, O> delegates(ItemProcessor... delegates) {
		if (delegates.length == 0) {
			return null;
		}
		if (delegates.length == 1) {
			return delegates[0];
		}
		CompositeItemStreamItemProcessor<I, O> processor = new CompositeItemStreamItemProcessor<>();
		processor.setDelegates((List) Arrays.asList(delegates));
		return processor;
	}

}
