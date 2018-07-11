package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NilWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private int currentItemCount = 0;

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		currentItemCount += items.size();
		log.info("NoOpped {} items", currentItemCount);
	}
}
