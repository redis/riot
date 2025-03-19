package com.redis.riot.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class HashToMapFunction implements Function<Map<String, String>, Map<String, String>> {

	@Override
	public Map<String, String> apply(Map<String, String> t) {
		Map<String, String> map = new LinkedHashMap<>();
		List<String> keys = new ArrayList<>(t.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			map.put(key, t.get(key));
		}
		return map;
	}

}
