package com.redislabs.riot.redis.writer.search;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.RedisConverter;

import io.redisearch.Suggestion;
import io.redisearch.client.Client;
import lombok.Setter;

public class JedisSuggestWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private Client client;
	private RedisConverter converter;
	@Setter
	private String field;
	@Setter
	private String scoreField;
	@Setter
	private double defaultScore = 1d;
	@Setter
	private boolean increment;
	@Setter
	private String payloadField;

	public JedisSuggestWriter(Client client, RedisConverter converter) {
		setName(ClassUtils.getShortName(JedisSuggestWriter.class));
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

}
