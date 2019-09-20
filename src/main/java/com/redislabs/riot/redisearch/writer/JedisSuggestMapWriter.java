package com.redislabs.riot.redisearch.writer;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.RedisConverter;

import io.redisearch.Suggestion;
import io.redisearch.client.Client;

public class JedisSuggestMapWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private Client client;
	private RedisConverter converter;
	private String field;
	private String scoreField;
	private double defaultScore = 1d;
	private boolean increment;
	private String payloadField;

	public JedisSuggestMapWriter(Client client, RedisConverter converter) {
		setName(ClassUtils.getShortName(JedisSuggestMapWriter.class));
		this.client = client;
		this.converter = converter;
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		for (Map<String, Object> item : items) {
			String string = converter.convert(item.get(field), String.class);
			if (string == null) {
				continue;
			}
			double score = converter.convert(item.getOrDefault(scoreField, defaultScore), Double.class);
			Suggestion suggestion = Suggestion.builder().payload(payload(item)).score(score).str(string).build();
			client.addSuggestion(suggestion, increment);
		}
	}

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return converter.convert(item.remove(payloadField), String.class);
	}

	public void setField(String field) {
		this.field = field;
	}

	public void setScoreField(String scoreField) {
		this.scoreField = scoreField;
	}

	public void setDefaultScore(double defaultScore) {
		this.defaultScore = defaultScore;
	}

	public void setIncrement(boolean increment) {
		this.increment = increment;
	}

	public void setPayloadField(String payloadField) {
		this.payloadField = payloadField;
	}

}
