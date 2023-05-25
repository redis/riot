package com.redis.riot.cli.operation;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.ObjectUtils;

import com.redis.riot.core.convert.MapFilteringConverter;
import com.redis.riot.core.convert.MapFilteringConverter.MapFilteringConverterBuilder;
import com.redis.riot.core.convert.MapFlattener;
import com.redis.riot.core.convert.ObjectToStringConverter;

import picocli.CommandLine.Option;

public class FilteringOptions {

	@Option(arity = "1..*", names = "--include", description = "Fields to include.", paramLabel = "<field>")
	private String[] includes;
	
	@Option(arity = "1..*", names = "--exclude", description = "Fields to exclude.", paramLabel = "<field>")
	private String[] excludes;

	public Function<Map<String, Object>, Map<String, String>> converter() {
		Function<Map<String, Object>, Map<String, String>> mapFlattener = new MapFlattener<>(
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
		return mapFlattener.andThen(filtering.build());
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
