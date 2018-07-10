package com.redislabs.recharge.dummy;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DummyItemWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private int currentItemCount = 0;

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		currentItemCount += items.size();
		log.info("NoOpped {} items", currentItemCount);
	}

}
