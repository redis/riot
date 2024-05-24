package com.redis.riot.function;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.lettuce.core.ScoredValue;

public class ZsetToMapFunction implements Function<Set<ScoredValue<String>>, Map<String, String>> {

	public static final String DEFAULT_SCORE_KEY_FORMAT = "score[%s]";
	public static final String DEFAULT_VALUE_KEY_FORMAT = "value[%s]";
	public static final String DEFAULT_SCORE_FORMAT = "%s";

	private String scoreKeyFormat = DEFAULT_SCORE_KEY_FORMAT;
	private String valueKeyFormat = DEFAULT_VALUE_KEY_FORMAT;
	private String scoreFormat = DEFAULT_SCORE_FORMAT;

	public void setScoreKeyFormat(String scoreKeyFormat) {
		this.scoreKeyFormat = scoreKeyFormat;
	}

	public void setValueKeyFormat(String valueKeyFormat) {
		this.valueKeyFormat = valueKeyFormat;
	}

	public void setScoreFormat(String scoreFormat) {
		this.scoreFormat = scoreFormat;
	}

	@Override
	public Map<String, String> apply(Set<ScoredValue<String>> source) {
		Map<String, String> result = new HashMap<>();
		int index = 0;
		for (ScoredValue<String> scoredValue : source) {
			result.put(String.format(scoreKeyFormat, index), String.format(scoreFormat, scoredValue.getScore()));
			result.put(String.format(valueKeyFormat, index), scoredValue.getValue());
			index++;
		}
		return result;
	}

}
