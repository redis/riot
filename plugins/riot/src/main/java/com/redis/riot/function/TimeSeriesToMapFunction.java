package com.redis.riot.function;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.redis.lettucemod.timeseries.Sample;

public class TimeSeriesToMapFunction implements Function<Collection<Sample>, Map<String, String>> {

	@Override
	public Map<String, String> apply(Collection<Sample> t) {
		Map<String, String> result = new HashMap<>();
		int index = 0;
		for (Sample sample : t) {
			result.put(String.valueOf(index), sample.getTimestamp() + ":" + sample.getValue());
			index++;
		}
		return result;
	}

}
