package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;

import com.redis.riot.convert.CompositeConverter;
import com.redis.riot.convert.MapFilteringConverter;
import com.redis.riot.convert.MapFilteringConverter.MapFilteringConverterBuilder;
import com.redis.riot.convert.MapFlattener;
import com.redis.riot.convert.ObjectToStringConverter;

import picocli.CommandLine;

public class FilteringOptions {

	@CommandLine.Option(arity = "1..*", names = "--include", description = "Fields to include", paramLabel = "<field>")
	private String[] includes;
	@CommandLine.Option(arity = "1..*", names = "--exclude", description = "Fields to exclude", paramLabel = "<field>")
	private String[] excludes;

	public Converter<Map<String, Object>, Map<String, String>> converter() {
		Converter<Map<String, Object>, Map<String, String>> mapFlattener = new MapFlattener<>(
				new ObjectToStringConverter());
		if (ObjectUtils.isEmpty(includes) && ObjectUtils.isEmpty(excludes)) {
			return mapFlattener;
		}
		MapFilteringConverterBuilder<String, String> filtering = MapFilteringConverter.builder();
		if (!ObjectUtils.isEmpty(includes)) {
			filtering.includes(includes);
		}
		if (!ObjectUtils.isEmpty(excludes)) {
			filtering.excludes(excludes);
		}
		return new CompositeConverter<>(mapFlattener, filtering.build());
	}

}
