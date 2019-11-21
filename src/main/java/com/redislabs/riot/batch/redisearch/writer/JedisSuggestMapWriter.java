package com.redislabs.riot.batch.redisearch.writer;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.util.ClassUtils;

import io.redisearch.Suggestion;
import io.redisearch.client.Client;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class JedisSuggestMapWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	@Setter
	private Client client;
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
	private ConversionService conversionService = new DefaultConversionService();

	public JedisSuggestMapWriter(Client client) {
		setName(ClassUtils.getShortName(JedisSuggestMapWriter.class));
		this.client = client;
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		for (Map<String, Object> item : items) {
			String string = conversionService.convert(item.get(field), String.class);
			if (string == null) {
				continue;
			}
			double score = conversionService.convert(item.getOrDefault(scoreField, defaultScore), Double.class);
			Suggestion suggestion = Suggestion.builder().payload(payload(item)).score(score).str(string).build();
			client.addSuggestion(suggestion, increment);
		}
	}

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return conversionService.convert(item.remove(payloadField), String.class);
	}

}
