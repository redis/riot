package com.redis.riot.processor;

import java.util.List;
import java.util.Optional;

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

	public static <I, O> Optional<ItemProcessor<I, O>> delegates(List<? extends ItemProcessor<I, O>> delegates) {
		if (delegates.isEmpty()) {
			return Optional.empty();
		}
		if (delegates.size() == 1) {
			return Optional.of(delegates.get(0));
		}
		CompositeItemStreamItemProcessor<I, O> processor = new CompositeItemStreamItemProcessor<>();
		processor.setDelegates(delegates);
		return Optional.of(processor);
	}

}
