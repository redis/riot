package com.redislabs.recharge.dummy;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class DummyItemWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private Logger log = LoggerFactory.getLogger(DummyItemWriter.class);

	private int currentItemCount = 0;

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		currentItemCount += items.size();
		log.info("NoOpped {} items", currentItemCount);
	}

}
