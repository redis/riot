package com.redislabs.recharge;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;

public class MapItemProcessor implements ItemProcessor<Map<String, String>, HashItem> {

	private String keyPrefix;
	private String[] keyFields;
	private String keySeparator;

	public MapItemProcessor(String keyPrefix, String[] keyFields, String keySeparator) {
		this.keyPrefix = keyPrefix;
		this.keyFields = keyFields;
		this.keySeparator = keySeparator;
	}

	@Override
	public HashItem process(Map<String, String> item) throws Exception {
		String key = getKey(item);
		return new HashItem(key, item);
	}

	private String getKey(Map<String, String> map) {
		String key = keyPrefix;
		for (String keyField : keyFields) {
			key += keySeparator + map.get(keyField);
		}
		return key;
	}

}
