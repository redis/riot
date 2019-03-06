package com.redislabs.recharge.dummy;

import java.util.List;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DummyWriter<T> extends AbstractItemStreamItemWriter<T> {

	private long count;

	@Override
	public void write(List<? extends T> items) throws Exception {
		log.info("Writing");
		count += items.size();
	}

	@Override
	public void close() {
		super.close();
	}

}
