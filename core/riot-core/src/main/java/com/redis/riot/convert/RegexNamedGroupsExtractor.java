package com.redis.riot.convert;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.core.convert.converter.Converter;

public class RegexNamedGroupsExtractor implements Converter<String, Map<String, String>> {

	private static final String NAMED_GROUPS = "\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>";

	private Pattern pattern;
	private Set<String> namedGroups;

	public RegexNamedGroupsExtractor(Pattern pattern, Set<String> namedGroups) {
		this.pattern = pattern;
		this.namedGroups = namedGroups;
	}

	public void setPattern(Pattern pattern) {
		this.pattern = pattern;
	}

	public void setNamedGroups(Set<String> namedGroups) {
		this.namedGroups = namedGroups;
	}

	public static RegexNamedGroupsExtractor of(String regex) {
		Pattern pattern = Pattern.compile(regex);
		Set<String> namedGroups = new TreeSet<>();
		Matcher m = Pattern.compile(NAMED_GROUPS).matcher(regex);
		while (m.find()) {
			namedGroups.add(m.group(1));
		}
		return new RegexNamedGroupsExtractor(pattern, namedGroups);
	}

	@Override
	public Map<String, String> convert(String source) {
		Map<String, String> fields = new HashMap<>();
		Matcher matcher = pattern.matcher(source);
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
