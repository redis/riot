package com.redislabs.riot.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

public class RegexProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

	private final Pattern NAMED_GROUP_PATTERN = Pattern.compile("(?<!\\\\)\\((\\?<(\\w+)>)?");
	private ConversionService converter = new DefaultConversionService();
	private Map<String, Pattern> patterns = new HashMap<>();
	private Map<String, List<String>> groupNames = new HashMap<>();

	public RegexProcessor(Map<String, String> map) {
		map.forEach((k, v) -> patterns.put(k, Pattern.compile(v)));
		map.forEach((k, v) -> groupNames.put(k, extractGroupNames(v)));
	}

	private List<String> extractGroupNames(String regex) {
		List<String> groupNames = new ArrayList<>();
		Matcher matcher = NAMED_GROUP_PATTERN.matcher(regex);
		while (matcher.find()) {
			groupNames.add(matcher.group(2));
		}
		return groupNames;
	}

	@Override
	public Map<String, Object> process(Map<String, Object> item) throws Exception {
		for (Entry<String, Pattern> pattern : patterns.entrySet()) {
			String input = converter.convert(item.get(pattern.getKey()), String.class);
			if (input==null) {
				continue;
			}
			Matcher matcher = pattern.getValue().matcher(input);
			if (matcher.find()) {
				groupNames.get(pattern.getKey()).forEach(name -> item.put(name, matcher.group(name)));
			}
		}
		return item;
	}

}
