package com.redislabs.riot.processor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.riot.redis.reader.FieldExtractor;

public class RegexProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private ConversionService converter = new DefaultConversionService();
	private Map<String, FieldExtractor> extractors = new LinkedHashMap<>();

	public RegexProcessor(Map<String, String> regexes) {
		regexes.forEach((k, v) -> extractors.put(k, FieldExtractor.builder().regex(v).build()));
	}

	@Override
	public Map<String, Object> process(Map<String, Object> item) throws Exception {
		for (Entry<String, FieldExtractor> field : extractors.entrySet()) {
			String input = converter.convert(item.get(field.getKey()), String.class);
			if (input == null) {
				continue;
			}
			Map<String, String> fields = field.getValue().getFields(input);
			item.putAll(fields);
		}
		return item;
	}

}
