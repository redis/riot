package com.redis.riot.core.processor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexNamedGroupFunction implements Function<String, Map<String, String>> {

	private static final String NAMED_GROUPS = "\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>";

	private final Pattern pattern;
	private final Set<String> namedGroups;

	public RegexNamedGroupFunction(Pattern pattern) {
		this.pattern = pattern;
		this.namedGroups = new TreeSet<>();
		Matcher m = Pattern.compile(NAMED_GROUPS).matcher(pattern.pattern());
		while (m.find()) {
			namedGroups.add(m.group(1));
		}
	}

	@Override
	public Map<String, String> apply(String string) {
		Matcher matcher = pattern.matcher(string);
		if (matcher.find()) {
			Map<String, String> fields = new HashMap<>();
			for (String name : namedGroups) {
				fields.put(name, matcher.group(name));
			}
			return fields;
		}
		return Collections.emptyMap();
	}

}
