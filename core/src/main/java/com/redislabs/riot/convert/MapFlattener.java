package com.redislabs.riot.convert;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Flattens a nested map using . and [] notation for key names
 *
 */
public class MapFlattener<T> implements Converter<Map<String, Object>, Map<String, T>> {

	private final Converter<Object, T> elementConverter;

	public MapFlattener(Converter<Object, T> elementConverter) {
		this.elementConverter = elementConverter;
	}

	@Override
	public Map<String, T> convert(Map<String, Object> source) {
		Map<String, T> resultMap = new LinkedHashMap<>();
		flatten("", source.entrySet().iterator(), resultMap);
		return resultMap;
	}

	private void flatten(String prefix, Iterator<? extends Entry<String, Object>> map, Map<String, T> flatMap) {
		String actualPrefix = StringUtils.hasText(prefix) ? prefix.concat(".") : prefix;
		while (map.hasNext()) {
			Entry<String, Object> element = map.next();
			flattenElement(actualPrefix.concat(element.getKey()), element.getValue(), flatMap);
		}
	}

	@SuppressWarnings("unchecked")
	private void flattenElement(String propertyPrefix, @Nullable Object source, Map<String, T> flatMap) {
		if (source == null) {
			return;
		}
		if (source instanceof Iterable) {
			int counter = 0;
			for (Object element : (Iterable<Object>) source) {
				flattenElement(propertyPrefix + "[" + counter + "]", element, flatMap);
				counter++;
			}
		} else if (source instanceof Map) {
			flatten(propertyPrefix, ((Map<String, Object>) source).entrySet().iterator(), flatMap);
		} else {
			((Map<String, Object>) flatMap).put(propertyPrefix, elementConverter.convert(source));
		}
	}

}
