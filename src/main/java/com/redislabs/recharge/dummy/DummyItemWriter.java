package com.redislabs.recharge.dummy;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redislabs.recharge.redis.HashItem;

public class DummyItemWriter extends AbstractItemStreamItemWriter<HashItem> {

	private Logger log = LoggerFactory.getLogger(DummyItemWriter.class);

	private int currentItemCount = 0;
	
	public void write(List<? extends HashItem> items) throws Exception {
		currentItemCount += items.size();
		log.info("NoOpped {} items", currentItemCount);
	}

}
