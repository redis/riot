package com.redis.riot.core.convert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.timeseries.Sample;

public class TimeSeriesToStringMapConverter implements Converter<List<Sample>, Map<String, String>> {

	@Override
	public Map<String, String> convert(List<Sample> source) {
		Map<String, String> result = new HashMap<>();
		for (int index = 0; index < source.size(); index++) {
			Sample sample = source.get(index);
			result.put(String.valueOf(index), sample.getTimestamp() + ":" + sample.getValue());
		}
		return result;
	}
}
