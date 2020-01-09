package com.redislabs.riot.processor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;

public class MapFlattener implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	@Override
	public Map<String, Object> process(Map<String, Object> item) throws Exception {
		Map<String, Object> flatMap = new HashMap<>();
		item.forEach((k, v) -> flatMap.putAll(flatten(k, v)));
		return flatMap;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, Object> flatten(String key, Object value) {
		Map<String, Object> flatMap = new HashMap<String, Object>();
		if (value instanceof Map) {
			((Map<String, Object>) value).forEach((k, v) -> {
				flatMap.putAll(flatten(key + "." + k, v));
			});
		} else {
			if (value instanceof Collection) {
				Collection collection = (Collection) value;
				Iterator iterator = collection.iterator();
				int index = 0;
				while (iterator.hasNext()) {
					flatMap.putAll(flatten(key + "[" + index + "]", iterator.next()));
					index++;
				}
			} else {
				flatMap.put(key, value);
			}
		}
		return flatMap;
	}

}
