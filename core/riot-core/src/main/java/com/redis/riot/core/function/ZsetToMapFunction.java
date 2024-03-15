package com.redis.riot.core.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.lettuce.core.ScoredValue;

public class ZsetToMapFunction implements Function<List<ScoredValue<String>>, Map<String, String>> {

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
	public Map<String, String> apply(List<ScoredValue<String>> source) {
		Map<String, String> result = new HashMap<>();
		for (int index = 0; index < source.size(); index++) {
			ScoredValue<String> scoredValue = source.get(index);
			result.put(String.format(scoreKeyFormat, index), String.format(scoreFormat, scoredValue.getScore()));
			result.put(String.format(valueKeyFormat, index), scoredValue.getValue());
		}
		return result;
	}

}
