package com.redis.riot.core.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

public class MapFilteringConverter implements UnaryOperator<Map<String, String>> {

	private final Set<String> includes;
	private final Set<String> excludes;

	public MapFilteringConverter(Set<String> includes, Set<String> excludes) {
		this.includes = includes;
		this.excludes = excludes;
	}

	@Override
	public Map<String, String> apply(Map<String, String> source) {
		Map<String, String> filtered = ObjectUtils.isEmpty(includes) ? source : new LinkedHashMap<>();
		includes.forEach(f -> filtered.put(f, source.get(f)));
		excludes.forEach(filtered::remove);
		return filtered;
	}

	public static MapFilteringConverterBuilder builder() {
		return new MapFilteringConverterBuilder();
	}

	public static class MapFilteringConverterBuilder {

		private List<String> includes = new ArrayList<>();
		private List<String> excludes = new ArrayList<>();

		public MapFilteringConverterBuilder includes(String... fields) {
			Assert.notNull(fields, "Fields cannot be null");
			this.includes = Arrays.asList(fields);
			return this;
		}

		public MapFilteringConverterBuilder excludes(String... fields) {
			Assert.notNull(fields, "Fields cannot be null");
			this.excludes = Arrays.asList(fields);
			return this;
		}

		public MapFilteringConverter build() {
			return new MapFilteringConverter(new LinkedHashSet<>(includes), new LinkedHashSet<>(excludes));
		}

	}
}
