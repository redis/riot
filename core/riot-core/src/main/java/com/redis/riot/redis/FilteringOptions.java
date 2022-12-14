package com.redis.riot.redis;

import java.util.Arrays;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;

import com.redis.riot.convert.CompositeConverter;
import com.redis.riot.convert.MapFilteringConverter;
import com.redis.riot.convert.MapFilteringConverter.MapFilteringConverterBuilder;
import com.redis.riot.convert.MapFlattener;
import com.redis.riot.convert.ObjectToStringConverter;

import picocli.CommandLine.Option;

public class FilteringOptions {

	@Option(arity = "1..*", names = "--include", description = "Fields to include.", paramLabel = "<field>")
	private String[] includes;
	@Option(arity = "1..*", names = "--exclude", description = "Fields to exclude.", paramLabel = "<field>")
	private String[] excludes;

	public Converter<Map<String, Object>, Map<String, String>> converter() {
		Converter<Map<String, Object>, Map<String, String>> mapFlattener = new MapFlattener<>(
				new ObjectToStringConverter());
		if (ObjectUtils.isEmpty(includes) && ObjectUtils.isEmpty(excludes)) {
			return mapFlattener;
		}
		MapFilteringConverterBuilder filtering = MapFilteringConverter.builder();
		if (!ObjectUtils.isEmpty(includes)) {
			filtering.includes(includes);
		}
		if (!ObjectUtils.isEmpty(excludes)) {
			filtering.excludes(excludes);
		}
		return new CompositeConverter<>(mapFlattener, filtering.build());
	}

	public String[] getIncludes() {
		return includes;
	}

	@Override
	public String toString() {
		return "FilteringOptions [includes=" + Arrays.toString(includes) + ", excludes=" + Arrays.toString(excludes)
				+ "]";
	}

	public void setIncludes(String[] includes) {
		this.includes = includes;
	}

	public String[] getExcludes() {
		return excludes;
	}

	public void setExcludes(String[] excludes) {
		this.excludes = excludes;
	}

}
