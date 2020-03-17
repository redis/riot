package com.redislabs.riot.redis.reader;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Builder;
import lombok.Data;

public @Data class FieldExtractor {

	private final static String REGEX = "\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>";

	private Pattern pattern;
	private Set<String> namedGroups;

	@Builder
	private FieldExtractor(String regex) {
		this.pattern = Pattern.compile(regex);
		this.namedGroups = new TreeSet<String>();
		Matcher m = Pattern.compile(REGEX).matcher(regex);
		while (m.find()) {
			namedGroups.add(m.group(1));
		}
	}

	public Map<String, String> getFields(String string) {
		Map<String, String> fields = new HashMap<>();
		Matcher matcher = pattern.matcher(string);
		if (matcher.find()) {
			for (String name : namedGroups) {
				String value = matcher.group(name);
				if (value != null) {
					fields.put(name, value);
				}
			}
		}
		return fields;
	}

}
