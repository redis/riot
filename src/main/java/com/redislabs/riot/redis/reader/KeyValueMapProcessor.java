package com.redislabs.riot.redis.reader;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.batch.item.ItemProcessor;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class KeyValueMapProcessor implements ItemProcessor<KeyValue, Map<String, Object>> {

	@SuppressWarnings({ "rawtypes" })
	@Override
	public Map process(KeyValue item) throws Exception {
		return getMap(item);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map getMap(KeyValue item) {
		switch (item.getType()) {
		case HASH:
			return (Map<String, Object>) item.getValue();
		case LIST:
			return toMap(((List<String>) item.getValue()).iterator());
		case SET:
			return toMap(((Set<String>) item.getValue()).iterator());
		case ZSET:
			return toMap((List<ScoredValue<String>>) item.getValue());
		case STREAM:
			return ((StreamMessage<String, String>) item.getValue()).getBody();
		case STRING:
			return toMap("value", item.getValue());
		}
		return null;
	}

	private Map<String, Object> toMap(String key, Object value) {
		Map<String, Object> map = new HashMap<>();
		map.put(key, value);
		return map;
	}

	private Map<String, Object> toMap(List<ScoredValue<String>> values) {
		Map<String, Object> map = new LinkedHashMap<>();
		for (ScoredValue<String> value : values) {
			map.put(value.getValue(), value.getScore());
		}
		return map;
	}

	private Map<String, String> toMap(Iterator<String> iterator) {
		Map<String, String> map = new LinkedHashMap<>();
		int index = 0;
		while (iterator.hasNext()) {
			map.put(String.valueOf(index), iterator.next());
			index++;
		}
		return map;
	}

}
