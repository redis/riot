package com.redislabs.riot.convert.map;

import lombok.Builder;
import org.springframework.core.convert.converter.Converter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexNamedGroupsExtractor implements Converter<String, Map<String, String>> {

    private final static String NAMED_GROUPS = "\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>";

    private final Pattern pattern;
    private final Set<String> namedGroups;

    @Builder
    private RegexNamedGroupsExtractor(String regex) {
        this.pattern = Pattern.compile(regex);
        this.namedGroups = new TreeSet<>();
        Matcher m = Pattern.compile(NAMED_GROUPS).matcher(regex);
        while (m.find()) {
            namedGroups.add(m.group(1));
        }
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
