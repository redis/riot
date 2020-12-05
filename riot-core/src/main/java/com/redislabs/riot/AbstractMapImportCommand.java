package com.redislabs.riot;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.support.ClientUtils;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.core.convert.converter.Converter;

import com.redislabs.riot.convert.RegexNamedGroupsExtractor;
import com.redislabs.riot.processor.MapProcessor;
import com.redislabs.riot.processor.SpelProcessor;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command
public abstract class AbstractMapImportCommand<I, O> extends AbstractImportCommand<I, O> {

	@Mixin
	private MapProcessingOptions options = new MapProcessingOptions();

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor(AbstractRedisClient client)
			throws Exception {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (!options.getSpelFields().isEmpty()) {
			processors.add(new SpelProcessor(ClientUtils.connection(client),
					new SimpleDateFormat(options.getDateFormat()), options.getVariables(), options.getSpelFields()));
		}
		if (!options.getRegexes().isEmpty()) {
			Map<String, Converter<String, Map<String, String>>> fields = new LinkedHashMap<>();
			options.getRegexes().forEach((f, r) -> fields.put(f, RegexNamedGroupsExtractor.builder().regex(r).build()));
			processors.add(new MapProcessor(fields));
		}
		if (processors.isEmpty()) {
			return null;
		}
		if (processors.size() == 1) {
			return processors.get(0);
		}
		CompositeItemProcessor<Map<String, Object>, Map<String, Object>> compositeItemProcessor = new CompositeItemProcessor<>();
		compositeItemProcessor.setDelegates(processors);
		return compositeItemProcessor;
	}
}
