package com.redislabs.recharge.dummy;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;

import com.redislabs.recharge.redis.HashItem;

public class DummyItemProcessor implements ItemProcessor<Map<String, String>, HashItem> {

	public HashItem process(Map<String, String> item) throws Exception {
		String key = item.get(DummyItemReader.FIELD);
		return new HashItem(key, item);
	}
}
