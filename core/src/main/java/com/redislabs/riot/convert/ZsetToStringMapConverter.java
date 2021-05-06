package com.redislabs.riot.convert;

import io.lettuce.core.ScoredValue;
import lombok.Builder;
import org.springframework.core.convert.converter.Converter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZsetToStringMapConverter implements Converter<List<ScoredValue<String>>, Map<String, String>> {

	public static final String DEFAULT_SCORE_KEY_FORMAT = "score[%s]";
	public static final String DEFAULT_VALUE_KEY_FORMAT = "value[%s]";
	public static final String DEFAULT_SCORE_FORMAT = "%s";

	private final String scoreKeyFormat = DEFAULT_SCORE_KEY_FORMAT;
	private final String valueKeyFormat = DEFAULT_VALUE_KEY_FORMAT;
	private final String scoreFormat = DEFAULT_SCORE_FORMAT;

	@Override
	public Map<String, String> convert(List<ScoredValue<String>> source) {
		Map<String, String> result = new HashMap<>();
		for (int index = 0; index < source.size(); index++) {
			ScoredValue<String> scoredValue = source.get(index);
			result.put(String.format(scoreKeyFormat, index), String.format(scoreFormat, scoredValue.getScore()));
			result.put(String.format(valueKeyFormat, index), scoredValue.getValue());
		}
		return result;
	}
}
