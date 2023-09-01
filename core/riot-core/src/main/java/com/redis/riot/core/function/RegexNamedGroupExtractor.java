package com.redis.riot.core.function;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexNamedGroupExtractor implements Function<String, Map<String, String>> {

    private static final String NAMED_GROUPS = "\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>";

    private final Pattern pattern;

    private final Set<String> namedGroups;

    public RegexNamedGroupExtractor(String regex) {
        this.pattern = Pattern.compile(regex);
        this.namedGroups = new TreeSet<>();
        Matcher m = Pattern.compile(NAMED_GROUPS).matcher(regex);
        while (m.find()) {
            namedGroups.add(m.group(1));
        }
    }

    @Override
    public Map<String, String> apply(String string) {
        Map<String, String> fields = new HashMap<>();
        Matcher matcher = pattern.matcher(string);
        if (matcher.find()) {
            for (String name : namedGroups) {
                fields.put(name, matcher.group(name));
            }
        }
        return fields;
    }

}
